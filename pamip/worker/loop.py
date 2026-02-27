import time

class WorkerLoop:
    def __init__(self, file_watcher, job_manager, pipeline_engine, interval=5):
        self.file_watcher = file_watcher
        self.job_manager = job_manager
        self.pipeline_engine = pipeline_engine
        self.interval = interval
        self.running = False

    def start(self):
        self.running = True
        print("Worker started.")

        while self.running:
            try:
                self._iteration()
            except Exception as e:
                print(f"Worker error: {e}")

            time.sleep(self.interval)

    def stop(self):
        self.running = False

    def _iteration(self):
        # 1️⃣ Detect new files
        new_files = self.file_watcher.scan_for_new_files()

        for file_path in new_files:
            self.job_manager.create_job(file_path)

        # 2️⃣ Process one job at a time (sequential model)
        job = self.job_manager.get_next_pending_job()

        if not job:
            return

        self.job_manager.mark_running(job)

        success = self.pipeline_engine.run(job)

        if success:
            self.job_manager.mark_completed(job)
        else:
            self.job_manager.mark_failed(job, error_message="Pipeline failed")