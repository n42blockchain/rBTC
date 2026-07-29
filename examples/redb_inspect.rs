use std::{env, error::Error, fs, path::Path};

use redb::{Database, ReadableTableMetadata, TableHandle};

fn main() -> Result<(), Box<dyn Error>> {
    let path = env::args().nth(1).ok_or("usage: redb_inspect PATH")?;
    let path = Path::new(&path);
    let file = fs::metadata(path)?;
    let database = Database::open(path)?;

    let write = database.begin_write()?;
    let stats = write.stats()?;
    println!(
        "database\tpath={}\tfile_bytes={}\tpage_size={}\tallocated_pages={}\tleaf_pages={}\tbranch_pages={}\tstored_bytes={}\tmetadata_bytes={}\tfragmented_bytes={}\ttree_height={}",
        path.display(),
        file.len(),
        stats.page_size(),
        stats.allocated_pages(),
        stats.leaf_pages(),
        stats.branch_pages(),
        stats.stored_bytes(),
        stats.metadata_bytes(),
        stats.fragmented_bytes(),
        stats.tree_height(),
    );
    drop(write);

    let read = database.begin_read()?;
    let mut handles = read.list_tables()?.collect::<Vec<_>>();
    handles.sort_by(|left, right| left.name().cmp(right.name()));
    for handle in handles {
        let name = handle.name().to_owned();
        let table = read.open_untyped_table(handle)?;
        let stats = table.stats()?;
        println!(
            "table\tname={name}\tentries={}\tleaf_pages={}\tbranch_pages={}\tstored_bytes={}\tmetadata_bytes={}\tfragmented_bytes={}\ttree_height={}",
            table.len()?,
            stats.leaf_pages(),
            stats.branch_pages(),
            stats.stored_bytes(),
            stats.metadata_bytes(),
            stats.fragmented_bytes(),
            stats.tree_height(),
        );
    }
    Ok(())
}
