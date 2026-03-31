# To-do List

## General
[] Ensure OS & pathing is universally compatible
[] Make different steps run for different file types; different pipelines, essentially
[] Make the processing of same-named files a config flag set to false by default (since the process involves moving them)
[] Adaptability: make move_file its own step run after all steps finish, without creating new jobs from the lingering file. Maybe tack it on the back of every job config
[] Add test runner / automation suite
[] What to do with non-compatible files? (probably just delete; could be a config option). Might cause issues with sidecar-type helper files?
[] support .img
[] support apple's live photos

## Restructuring
[] remove job_manager.py in favor of jobs/manager.py
[] remove core/
[] move config.py into config/
[] move tests to tests/
[x] remove worker/scheduler.py
