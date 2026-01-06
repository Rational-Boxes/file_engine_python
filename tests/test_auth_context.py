import unittest
from unittest.mock import Mock, patch
import grpc

from fileengine.client import ManagedFiles


class TestAuthContext(unittest.TestCase):
    """Test the authentication context creation in ManagedFiles"""
    
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

    def test_create_auth_context_defaults(self):
        """Test creating auth context with default values"""
        auth_context = self.mf._create_auth_context()
        
        self.assertEqual(auth_context.user, "test_user")
        self.assertEqual(auth_context.roles, ["user"])
        self.assertEqual(auth_context.tenant, "test_tenant")
        self.assertEqual(auth_context.claims, {"read": "read", "write": "write"})

    def test_create_auth_context_override_user(self):
        """Test creating auth context with overridden user"""
        auth_context = self.mf._create_auth_context(user="other_user")
        
        self.assertEqual(auth_context.user, "other_user")
        self.assertEqual(auth_context.roles, ["user"])  # Should keep default roles
        self.assertEqual(auth_context.tenant, "test_tenant")  # Should keep default tenant
        self.assertEqual(auth_context.claims, {"read": "read", "write": "write"})  # Should keep default claims

    def test_create_auth_context_override_tenant(self):
        """Test creating auth context with overridden tenant"""
        auth_context = self.mf._create_auth_context(tenant="other_tenant")
        
        self.assertEqual(auth_context.user, "test_user")  # Should keep default user
        self.assertEqual(auth_context.roles, ["user"])  # Should keep default roles
        self.assertEqual(auth_context.tenant, "other_tenant")
        self.assertEqual(auth_context.claims, {"read": "read", "write": "write"})  # Should keep default claims

    def test_create_auth_context_override_roles(self):
        """Test creating auth context with overridden roles"""
        auth_context = self.mf._create_auth_context(roles=["admin", "superuser"])
        
        self.assertEqual(auth_context.user, "test_user")  # Should keep default user
        self.assertEqual(auth_context.roles, ["admin", "superuser"])
        self.assertEqual(auth_context.tenant, "test_tenant")  # Should keep default tenant
        self.assertEqual(auth_context.claims, {"read": "read", "write": "write"})  # Should keep default claims

    def test_create_auth_context_override_claims_as_strings(self):
        """Test creating auth context with overridden claims as strings"""
        auth_context = self.mf._create_auth_context(claims=["execute", "delete"])
        
        self.assertEqual(auth_context.user, "test_user")  # Should keep default user
        self.assertEqual(auth_context.roles, ["user"])  # Should keep default roles
        self.assertEqual(auth_context.tenant, "test_tenant")  # Should keep default tenant
        self.assertEqual(auth_context.claims, {"execute": "execute", "delete": "delete"})

    def test_create_auth_context_override_claims_as_dict(self):
        """Test creating auth context with overridden claims as dict"""
        auth_context = self.mf._create_auth_context(claims=[{"permission1": "value1", "permission2": "value2"}])
        
        self.assertEqual(auth_context.user, "test_user")  # Should keep default user
        self.assertEqual(auth_context.roles, ["user"])  # Should keep default roles
        self.assertEqual(auth_context.tenant, "test_tenant")  # Should keep default tenant
        self.assertEqual(auth_context.claims, {"permission1": "value1", "permission2": "value2"})

    def test_create_auth_context_override_claims_as_tuples(self):
        """Test creating auth context with overridden claims as tuples"""
        auth_context = self.mf._create_auth_context(claims=[("perm1", "val1"), ("perm2", "val2")])
        
        self.assertEqual(auth_context.user, "test_user")  # Should keep default user
        self.assertEqual(auth_context.roles, ["user"])  # Should keep default roles
        self.assertEqual(auth_context.tenant, "test_tenant")  # Should keep default tenant
        self.assertEqual(auth_context.claims, {"perm1": "val1", "perm2": "val2"})

    def test_create_auth_context_override_all(self):
        """Test creating auth context with all overrides"""
        auth_context = self.mf._create_auth_context(
            user="new_user",
            tenant="new_tenant",
            roles=["new_role"],
            claims=["new_claim"]
        )
        
        self.assertEqual(auth_context.user, "new_user")
        self.assertEqual(auth_context.roles, ["new_role"])
        self.assertEqual(auth_context.tenant, "new_tenant")
        self.assertEqual(auth_context.claims, {"new_claim": "new_claim"})


if __name__ == '__main__':
    unittest.main()