import unittest
import time
from unittest.mock import Mock, patch
import grpc

from fileengine.client import ManagedFiles, FileType


class TestMoveCopyOperations(unittest.TestCase):
    """Test move and copy operations in the ManagedFiles class"""

    def setUp(self):
        """Set up test fixtures before each test method."""
        # Mock the gRPC channel and stub
        self.mock_channel = Mock()
        self.mock_stub = Mock()

        # Patch the gRPC channel creation
        with patch('fileengine.client.grpc.insecure_channel', return_value=self.mock_channel):
            with patch('fileengine.client.fileservice_pb2_grpc.FileServiceStub', return_value=self.mock_stub):
                self.mf = ManagedFiles(
                    user_name="test_user",
                    user_roles=["user"],
                    user_claims=["read", "write"],
                    server_address="localhost:50051",
                    tenant="test_tenant"
                )

                # Set the stub on the instance
                self.mf.stub = self.mock_stub

    def tearDown(self):
        """Clean up after each test method."""
        self.mf.close()

    def test_move_success(self):
        """Test successful move operation."""
        # Mock the Move response
        mock_response = Mock()
        mock_response.success = True
        self.mock_stub.Move.return_value = mock_response

        # Perform the move operation
        result = self.mf.move("source-uuid", "destination-uuid")

        # Verify the result
        self.assertTrue(result)

        # Verify that the Move method was called with correct parameters
        self.mock_stub.Move.assert_called_once()
        call_args = self.mock_stub.Move.call_args[0][0]
        self.assertEqual(call_args.source_uid, "source-uuid")
        self.assertEqual(call_args.destination_parent_uid, "destination-uuid")
        self.assertIsNotNone(call_args.auth)

    def test_move_with_rename_success(self):
        """Test successful move operation with rename."""
        # Mock the Move response
        move_response = Mock()
        move_response.success = True
        self.mock_stub.Move.return_value = move_response

        # Mock the Rename response
        rename_response = Mock()
        rename_response.success = True
        self.mock_stub.Rename.return_value = rename_response

        # Perform the move operation with rename
        result = self.mf.move("source-uuid", "destination-uuid", new_name="new_name")

        # Verify the result
        self.assertTrue(result)

        # Verify that both Move and Rename methods were called
        self.mock_stub.Move.assert_called_once()
        move_call_args = self.mock_stub.Move.call_args[0][0]
        self.assertEqual(move_call_args.source_uid, "source-uuid")
        self.assertEqual(move_call_args.destination_parent_uid, "destination-uuid")
        self.assertIsNotNone(move_call_args.auth)

        self.mock_stub.Rename.assert_called_once()
        rename_call_args = self.mock_stub.Rename.call_args[0][0]
        self.assertEqual(rename_call_args.uid, "source-uuid")
        self.assertEqual(rename_call_args.new_name, "new_name")
        self.assertIsNotNone(rename_call_args.auth)

    def test_move_failure(self):
        """Test move operation failure."""
        # Mock the Move response with failure
        mock_response = Mock()
        mock_response.success = False
        self.mock_stub.Move.return_value = mock_response

        # Perform the move operation
        result = self.mf.move("source-uuid", "destination-uuid")

        # Verify the result
        self.assertFalse(result)

        # Verify that the Move method was called with correct parameters
        self.mock_stub.Move.assert_called_once()
        call_args = self.mock_stub.Move.call_args[0][0]
        self.assertEqual(call_args.source_uid, "source-uuid")
        self.assertEqual(call_args.destination_parent_uid, "destination-uuid")
        self.assertIsNotNone(call_args.auth)

    def test_move_grpc_error(self):
        """Test move operation with gRPC error."""
        # Configure the stub to raise a gRPC error
        self.mock_stub.Move.side_effect = grpc.RpcError("gRPC error occurred")

        # Perform the move operation
        result = self.mf.move("source-uuid", "destination-uuid")

        # Verify the result
        self.assertFalse(result)

        # Verify that the Move method was called with correct parameters
        self.mock_stub.Move.assert_called_once()
        call_args = self.mock_stub.Move.call_args[0][0]
        self.assertEqual(call_args.source_uid, "source-uuid")
        self.assertEqual(call_args.destination_parent_uid, "destination-uuid")
        self.assertIsNotNone(call_args.auth)

    def test_copy_success(self):
        """Test successful copy operation."""
        # Mock the Copy response
        mock_response = Mock()
        mock_response.success = True
        self.mock_stub.Copy.return_value = mock_response

        # Perform the copy operation
        result = self.mf.copy("source-uuid", "destination-uuid")

        # Verify the result
        self.assertTrue(result)

        # Verify that the Copy method was called with correct parameters
        self.mock_stub.Copy.assert_called_once()
        call_args = self.mock_stub.Copy.call_args[0][0]
        self.assertEqual(call_args.source_uid, "source-uuid")
        self.assertEqual(call_args.destination_parent_uid, "destination-uuid")
        self.assertIsNotNone(call_args.auth)

    def test_copy_failure(self):
        """Test copy operation failure."""
        # Mock the Copy response with failure
        mock_response = Mock()
        mock_response.success = False
        self.mock_stub.Copy.return_value = mock_response

        # Perform the copy operation
        result = self.mf.copy("source-uuid", "destination-uuid")

        # Verify the result
        self.assertFalse(result)

        # Verify that the Copy method was called with correct parameters
        self.mock_stub.Copy.assert_called_once()
        call_args = self.mock_stub.Copy.call_args[0][0]
        self.assertEqual(call_args.source_uid, "source-uuid")
        self.assertEqual(call_args.destination_parent_uid, "destination-uuid")
        self.assertIsNotNone(call_args.auth)

    def test_copy_grpc_error(self):
        """Test copy operation with gRPC error."""
        # Configure the stub to raise a gRPC error
        self.mock_stub.Copy.side_effect = grpc.RpcError("gRPC error occurred")

        # Perform the copy operation
        result = self.mf.copy("source-uuid", "destination-uuid")

        # Verify the result
        self.assertFalse(result)

        # Verify that the Copy method was called with correct parameters
        self.mock_stub.Copy.assert_called_once()
        call_args = self.mock_stub.Copy.call_args[0][0]
        self.assertEqual(call_args.source_uid, "source-uuid")
        self.assertEqual(call_args.destination_parent_uid, "destination-uuid")
        self.assertIsNotNone(call_args.auth)


if __name__ == '__main__':
    unittest.main()