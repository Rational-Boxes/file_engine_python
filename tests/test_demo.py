import unittest
from unittest.mock import Mock, patch, MagicMock
import io
import sys
from contextlib import redirect_stdout

# Add the parent directory to the path to import demo
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import demo
from fileengine import ManagedFiles


class TestDemo(unittest.TestCase):
    """Test the demo script functionality"""

    @patch('demo.ManagedFiles')
    def test_demo_main(self, mock_managed_files_class):
        """Test the demo main function"""
        # Create mock ManagedFiles instance
        mock_mf_instance = Mock()

        # Set up the mock to return expected values
        mock_mf_instance.mkdir.return_value = "root-uuid"
        mock_mf_instance.touch.return_value = "file-uuid"
        mock_mf_instance.put.return_value = 1234567890.0
        mock_mf_instance.get.return_value = Mock()
        mock_mf_instance.get.return_value.getvalue.return_value = b"This is a demo file for the FileEngine Python client."
        mock_mf_instance.dir.return_value = []
        mock_mf_instance.revisions.return_value = [{'version': '1234567890.0', 'name': 'demo_file.txt', 'user': 'demo_user'}]
        mock_mf_instance.file_name.return_value = ['demo_file.txt']
        mock_mf_instance.get_file_mtime.return_value = None

        # Make the class constructor return our mock instance
        mock_managed_files_class.return_value = mock_mf_instance

        # Capture stdout to verify the demo output
        f = io.StringIO()
        with redirect_stdout(f):
            # Run the demo main function
            demo.main()

        output = f.getvalue()

        # Verify that the expected output is in the demo output
        self.assertIn("Created root directory", output)
        self.assertIn("Created file", output)
        self.assertIn("Written content", output)
        self.assertIn("Read content", output)
        self.assertIn("Demo completed successfully", output)


if __name__ == '__main__':
    unittest.main()