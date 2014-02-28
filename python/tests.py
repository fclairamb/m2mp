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

unittest.main()
