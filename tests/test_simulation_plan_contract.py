from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_legacy_simulation_worker_is_fully_retired():
    assert not (ROOT / "scripts/simulated-worker.py").exists()

    bootstrap = (ROOT / "scripts/bootstrap.sh").read_text(encoding="utf-8")
    status = (ROOT / "scripts/write-platform-status.sh").read_text(encoding="utf-8")
    frontend = (ROOT / "app/static/index.html").read_text(encoding="utf-8")
    api = (ROOT / "app/main.py").read_text(encoding="utf-8")

    assert "install_root/scripts/simulated-worker.py" not in bootstrap
    assert "ExecStart=/usr/bin/python3 /usr/local/sbin/s3-oci-simulated-worker" not in bootstrap
    assert '"simulated_worker"' not in status
    assert 'id="set-simulation"' not in frontend
    assert 'post("/api/tasks/{task_id}/simulate")' not in api
