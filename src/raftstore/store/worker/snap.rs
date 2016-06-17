// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

use raftstore::store::{self, RaftStorage, SnapState, SnapKey, SnapFile};

use std::fmt::{self, Formatter, Display};
use std::error;
use std::time::Instant;
use std::sync::{Arc, RwLock};

use util::worker::Runnable;
use util::HandyRwLock;

/// Snapshot generating task.
pub struct Task {
    region_id: u64,
    storage: RaftStorage,
    snap_state: Arc<RwLock<SnapState>>,
}

impl Task {
    pub fn new(region_id: u64, storage: RaftStorage, snap_state: Arc<RwLock<SnapState>>) -> Task {
        Task {
            region_id: region_id,
            storage: storage,
            snap_state: snap_state,
        }
    }
}

impl Display for Task {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "Snapshot Task for {}", self.region_id)
    }
}

quick_error! {
    #[derive(Debug)]
    enum Error {
        Other(err: Box<error::Error + Sync + Send>) {
            from()
            cause(err.as_ref())
            description(err.description())
            display("snap failed {:?}", err)
        }
    }
}

pub struct Runner {
    base_dir: String,
}

impl Runner {
    pub fn new(base_dir: &str) -> Runner {
        Runner { base_dir: base_dir.to_owned() }
    }

    fn generate_snap(&self, task: &Task) -> Result<(), Error> {
        // do we need to check leader here?
        let db = task.storage.rl().get_engine();
        let raw_snap;
        let ranges;
        let key;
        let mut snap_file;

        {
            let storage = task.storage.rl();
            raw_snap = db.snapshot();
            ranges = storage.region_key_ranges();
            let applied_idx = box_try!(storage.load_applied_index(&raw_snap));
            let term = box_try!(storage.term(applied_idx));
            key = SnapKey::new(storage.get_region_id(), term, applied_idx);
            snap_file = box_try!(SnapFile::new(self.base_dir.clone(), true, &key));
        }

        let snap = box_try!(store::do_snapshot(&mut snap_file, &raw_snap, &key, ranges));
        *task.snap_state.wl() = SnapState::Snap(snap);
        Ok(())
    }
}

impl Runnable<Task> for Runner {
    fn run(&mut self, task: Task) {
        metric_incr!("raftstore.generate_snap");
        let ts = Instant::now();
        if let Err(e) = self.generate_snap(&task) {
            error!("failed to generate snap: {:?}!!!", e);
            if let SnapState::Generating(cnt) = *task.snap_state.rl() {
                *task.snap_state.wl() = SnapState::Failed(cnt + 1);
            }
            return;
        }
        metric_incr!("raftstore.generate_snap.success");
        metric_time!("raftstore.generate_snap.cost", ts.elapsed());
    }
}
