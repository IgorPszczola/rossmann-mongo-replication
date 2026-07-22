import os
import tempfile
import unittest
from unittest.mock import patch

import pipeline


class PipelineStartupTest(unittest.TestCase):
    def test_listener_ready_file_tracks_listener_state(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            ready_path = os.path.join(temp_dir, "listener-ready")

            with patch.object(pipeline, "LISTENER_READY_FILE", ready_path):
                pipeline.set_listener_ready(True)
                self.assertTrue(os.path.exists(ready_path))

                pipeline.set_listener_ready(False)
                self.assertFalse(os.path.exists(ready_path))


if __name__ == "__main__":
    unittest.main()
