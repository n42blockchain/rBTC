//! Offline tool argument errors must fail before modifying operator data.

use std::process::Command;

#[test]
fn ledger_import_cli_rejects_bad_options_and_reports_missing_sources() {
    let directory = tempfile::tempdir().unwrap();
    let binary = env!("CARGO_BIN_EXE_fdb_ledger_import");
    let base = "00".repeat(32);
    let common = [
        "--src",
        "missing-source",
        "--out",
        "output",
        "--base-hash",
        &base,
        "--base-height",
        "0",
    ];
    for args in [
        vec![],
        vec!["--unknown"],
        vec!["--src"],
        vec!["--base-hash", "bad"],
        vec!["--base-height", "-1"],
        vec!["--slots", "65536"],
    ] {
        let output = Command::new(binary)
            .current_dir(directory.path())
            .args(args)
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(2));
        assert!(!output.stderr.is_empty());
    }
    for extra in [vec!["--slots", "0"], vec!["--segment-blocks", "0"]] {
        let output = Command::new(binary)
            .current_dir(directory.path())
            .args(common)
            .args(extra)
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(2));
    }
    for verify in [false, true] {
        let mut command = Command::new(binary);
        command.current_dir(directory.path()).args(common).args([
            "--max-height",
            "10",
            "--segment-blocks",
            "2",
            "--slots",
            "4",
            "--summary",
            "summary.json",
        ]);
        if verify {
            command.arg("--verify-existing");
        }
        let output = command.output().unwrap();
        assert_eq!(output.status.code(), Some(1));
        assert!(String::from_utf8_lossy(&output.stderr).contains("fdb_ledger_import:"));
        assert!(!directory.path().join("output").exists());
        assert!(!directory.path().join("summary.json").exists());
    }
}
