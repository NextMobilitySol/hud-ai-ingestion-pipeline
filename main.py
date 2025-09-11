from __future__ import annotations
import argparse, subprocess, sys

def run(mod: str, args: list[str]) -> int:
    """
    Delegar a cada módulo existente
    """
    cmd = [sys.executable, "-m", mod] + args
    return subprocess.call(cmd)

def main() -> None:
    parser = argparse.ArgumentParser(
        prog="hud",
        description="HUD Ingestion CLI (wrapper): upload, delete, reconcile"
    )
    sub = parser.add_subparsers(dest="cmd", required=True)

    # upload
    p_up = sub.add_parser("upload", help="Upload ZIP to GCS and register in BQ")
    p_up.add_argument("--zip", required=True, help="Local path to the ZIP file")
    p_up.add_argument("--origin", required=True, choices=["youtube","public","simulated","real"], help="ZIP source type")
    p_up.add_argument("--dataset", default="", help="Dataset logical name (if applicable)")
    p_up.add_argument("--url", default="", help="Source URL (required if origin=youtube)")

    # delete
    p_del = sub.add_parser("delete", help="Safe delete: remove ZIP from GCS and mark soft-delete in BQ")
    p_del.add_argument("--zip", required=True, help="ZIP file name (e.g., my_dataset_2025-08-31.zip)")
    p_del.add_argument("--origin", choices=["youtube","public","simulated","real"], help="Category where the ZIP lives (public|simulated|real|youtube). If omitted, it will be auto-resolved.")
    p_del.add_argument("--reason", default="cleanup", help="Reason for deletion (default: cleanup)")
    p_del.add_argument("--who", default="uploader-cli", help="Actor performing the deletion")

    # reconcile
    p_rec = sub.add_parser("reconcile", help="Reconcile Lake & Warehouse")
    p_rec.add_argument("--dry-run", action="store_true", help="Show actions without applying changes")
    p_rec.add_argument("--reason", default="reconcile-missing", help="Soft-delete reason to set when ZIP is missing in GCS")
    p_rec.add_argument("--who", default="reconcile-cli", help="Actor label for updates")
    p_rec.add_argument("--upload-log", action="store_true", help="Upload a JSON report to logs/archive_reconcile/")
    p_rec.add_argument("--include-deleted", action="store_true", help="Also analyze soft-deleted rows (report-only unless combined with --reactivate-deleted).")
    p_rec.add_argument("--reactivate-deleted", action="store_true",help="If a row is soft-deleted but the ZIP exists in GCS, reactivate it. Implies --include-deleted.")

    args = parser.parse_args()
    if args.cmd == "upload":
        code = run("src.uploader", [
            "--zip", args.zip,
            "--origin", args.origin,
            "--dataset", args.dataset,
            *([] if not args.url else ["--url", args.url]),
        ])
    elif args.cmd == "delete":
        base = ["--zip", args.zip, "--reason", args.reason, "--who", args.who]
        if args.origin:
            base += ["--origin", args.origin]
        code = run("src.delete_zip", base)
    elif args.cmd == "reconcile":
        base = [
            "--reason", args.reason,
            "--who", args.who,
        ]
        if args.dry_run: base.append("--dry-run")
        if args.upload_log: base.append("--upload-log")
        if args.include_deleted: base.append("--include-deleted")
        if args.reactivate_deleted: base.append("--reactivate-deleted")
        code = run("src.reconcile", base)
    else:
        code = 2
    sys.exit(code)

if __name__ == "__main__":
    main()
