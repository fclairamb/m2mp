import uuid
import os
from cassandra.cluster import Cluster


cluster = Cluster(['127.0.0.1'])
session = cluster.connect('ks_test')


class RegistryNode:
    STATUS_UNDEFINED = -1
    STATUS_DELETED = 5
    STATUS_CREATED = 100

    def __init__(self, path):
        if not path.endswith('/'):
            path += '/'
        self.path = path
        self.children_names = None
        self.properties = None
        self.status = None

    def get_name(self):
        return os.path.basename(os.path.normpath(self.path))

    def get_parent(self):
        return RegistryNode(os.path.dirname(os.path.normpath(self.path)))

    def get_child(self, name):
        return RegistryNode(self.path + name)

    def get_children_names(self):
        if self.children_names:
            return self.children_names
        rows = session.execute('select name from registrynodechildren where path=%s;', [self.path])
        self.children_names = [row[0] for row in rows]
        return self.get_children_names

    def add_child(self, name):
        session.execute('insert into registrynodechildren (path, name) values (%s, %s);', [ self.path, name ] )

    def remove_child(self, name):
        session.execute('delete from registrynodechildren where path = %s and name = %s', [ self.path, name ] )

    def get_children(self):
        children = []
        for n in self.get_children_names():
            children.append(RegistryNode("{path}{name}".format(path=self.path, name=n)))
        return children

    def get_status(self):
        for row in session.execute('select status from registrynode where path=%s;', [self.path]):
            return row[0]
        return None

    def get_properties(self):
        if self.properties:
            return self.properties

        for row in session.execute('select values from registrynode where path=%s;', [self.path]):
            self.properties = row[0]
            return self.properties

        self.properties = {}
        return self.properties

    def get_property(self, name):
        properties = self.get_properties()
        return properties.get(name)

    def set_property(self, name, value):
        session.execute('update registrynode set values [ %s ] = %s where path = %s;', [name, value, self.path])
        if self.properties:
            self.properties[name] = value

    def del_property(self, name):
        session.execute('delete values [ %s ] from registrynode where path = %s;', [name, self.path])
        if self.properties:
            self.properties.remove(name)

    def get_status(self):
        if self.status is None:
            for row in session.execute('select status from registrynode where path = %s;', [ self.path ]):
                self.status = row[0]
            if self.status is None:
                self.status = RegistryNode.STATUS_UNDEFINED

        return self.status

    def set_status(self, status):
        session.execute('update registrynode set status=%s where path=%s;', [status, self.path])

    def create(self):
        self.set_status(RegistryNode.STATUS_CREATED)
        parent = self.get_parent()
        if parent:
            parent.check()
            parent.add_child(self.get_name())

        return self

    def check(self):
        status = self.get_status()
        if status == RegistryNode.STATUS_UNDEFINED or status == RegistryNode.STATUS_DELETED:
            self.create()

        return self

    def exists(self):
        return self.get_status() == RegistryNode.STATUS_CREATED

    def delete(self, for_real=False):
        for child in self.get_children():
            child.delete(for_real)

        if for_real:
            session.execute('delete from registrynode where path=%s;', self.path)
            self.properties = None
        else:
            self.set_status(RegistryNode.STATUS_DELETED)

        parent = self.get_parent()
        if parent:
            parent.remove_child(self.get_name())

    def __str__(self):
        return "RegistryNode{{path={path}}}".format(path=self.path)


class Domain:
    PATH = '/domain/'

    PROPERTY_NAME = '__name'

    def __init__(self, id):
        self.id = id
        self.node = RegistryNode('{base}{id}/'.format(base=Domain.PATH, id=self.id))

    @staticmethod
    def get_by_name(name):
        for row in session.execute('select id from domain where name=%s;', [name]):
            return Domain(row[0])
        return None

    @staticmethod
    def get_by_name_or_create(name):
        ins = Domain.get_by_name(name)
        if ins:
            return ins
        id = uuid.uuid4()
        session.execute('insert into domain (name, id) values (%s,%s);', [name, id])
        domain = Domain(id)
        domain.node.check()
        domain.node.set_property(Domain.PROPERTY_NAME, name)
        return domain

    def get_name(self):
        return self.node.get_property(Domain.PROPERTY_NAME)

    def delete(self):
        name = self.get_name()
        if name:
            session.execute('delete from domain where name=%s;', [name])


class User:
    PATH = '/user/'

    def __init__(self, id):
        self.id = id
        self.node = RegistryNode('{base}{id}/'.format(base=User.PATH, id=self.id))

    @staticmethod
    def get_by_name(name):
        for row in session.execute('select id from user where name=%s;', [name]):
            return User(row[0])
        return None

    def get_domain(self):
        for row in session.execute('select domain from user where id=%s;', [self.id]):
            return Domain(row[0])
