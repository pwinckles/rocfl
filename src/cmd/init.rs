use crate::cmd::{Cmd, GlobalArgs, println};
use crate::cmd::opts::{Init, Layout, RocflArgs, Storage};
use crate::ocfl::layout::{LayoutExtensionName, StorageLayout};
use crate::ocfl::{OcflRepo, Result};

pub fn init_repo(cmd: &Init, args: &RocflArgs) -> Result<()> {
    match args.target_storage() {
        Storage::FileSystem => {
            let _ = OcflRepo::init_fs_repo(&args.root, create_layout(cmd.layout)?)?;
        }
        // TODO S3
        Storage::S3 => unimplemented!()
    }

    if !args.quiet {
        println("Initialized OCFL repository")?;
    }

    Ok(())
}

fn create_layout(layout: Layout) -> Result<StorageLayout> {
    match layout {
        Layout::FlatDirect => StorageLayout::new(LayoutExtensionName::FlatDirectLayout, None),
        Layout::HashedNTuple => StorageLayout::new(LayoutExtensionName::HashedNTupleLayout, None),
        Layout::HashedNTupleObjectId => StorageLayout::new(LayoutExtensionName::HashedNTupleObjectIdLayout, None),
    }
}

/// This is needed to keep enum_dispatch happy
impl Cmd for Init {
    fn exec(&self, _repo: &OcflRepo, _args: GlobalArgs) -> Result<()> {
        unimplemented!()
    }
}
