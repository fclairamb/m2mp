__author__ = 'florent'
import unittest
import models


class RegistryTestCase(unittest.TestCase):
    def test_name(self):
        r = models.RegistryNode('/dir1/dir2/dir3/')
        self.assertEqual(r.get_name(), 'dir3')

    def test_parent(self):
        r = models.RegistryNode('/dir1/dir2/dir3/')
        self.assertEqual(r.get_parent().get_name(), 'dir2')

    def test_simple_deletion(self):
        r = models.RegistryNode('/dir1/dir2/dir3/').check()
        r.delete()

        self.assertFalse(r.exists())


class DomainTestCase(unittest.TestCase):
    def setUp(self):
        domain = models.Domain.get_by_name('test')
        if domain:
            domain.delete()

    def test_domain_creation(self):
        domain = models.Domain.get_by_name('test')
        self.assertIsNone(domain)

        domain = models.Domain.get_by_name_or_create('test')
        self.assertIsNotNone(domain)

        domain = models.Domain.get_by_name('test')
        self.assertIsNotNone(domain)


class UserTestCase(unittest.TestCase):
    def setUp(self):
        user = models.User.get_by_name('test')
        if user:
            user.delete()

    def test_user_creation(self):
        user = models.User.get_by_name('test')
        self.assertIsNone(user)

        user = models.User.get_by_name_or_create('test')
        self.assertIsNotNone(user)

        user = models.User.get_by_name('test')
        self.assertIsNotNone(user)

        domain = user.get_domain()
        self.assertIsNotNone(user)
        domain_name = domain.get_name()
        self.assertEqual(domain_name, 'user_test')

        domain = models.Domain.get_by_name(domain_name)
        self.assertIsNotNone(domain)

unittest.main()
