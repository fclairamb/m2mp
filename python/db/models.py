import uuid
import os
import time
import datetime

from cassandra.cluster import Cluster


def set_session(keyspace='ks_test', hosts=['127.0.0.1']):
    global cluster, session
    cluster = Cluster(hosts)
    session = cluster.connect(keyspace)


class RegistryNode:
    STATUS_UNDEFINED = -1
    STATUS_DELETED = 5
    STATUS_CREATED = 100

    PROPERTY_DELETION_TIME = '.deletion-time'

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
        return self.children_names

    def add_child(self, name):
        session.execute('insert into registrynodechildren (path, name) values (%s, %s);', [self.path, name])

    def remove_child(self, name):
        session.execute('delete from registrynodechildren where path = %s and name = %s', [self.path, name])

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

        self.properties = {}
        for row in session.execute('select values from registrynode where path=%s;', [self.path]):
            self.properties = row[0]

            if not self.properties:
                self.properties = {}

            return self.properties

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
            for row in session.execute('select status from registrynode where path = %s;', [self.path]):
                self.status = row[0]
            if self.status is None:
                self.status = RegistryNode.STATUS_UNDEFINED

        return self.status

    def set_status(self, status):
        session.execute('update registrynode set status=%s where path=%s;', [status, self.path])
        self.status = status

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
            # Values aren't changed, it's pretty easy to undelete some data at this stage
            self.set_status(RegistryNode.STATUS_DELETED)
            self.set_property(RegistryNode.PROPERTY_DELETION_TIME, str(int(time.time())))

        parent = self.get_parent()
        if parent:
            parent.remove_child(self.get_name())

    def __str__(self):
        return "RegistryNode{{path={path}}}".format(path=self.path)


class Domain:
    PATH = '/domain/'
    PATH_BY_NAME = PATH + 'by-name/'

    def __init__(self, id):
        self.id = id
        self.node = RegistryNode('{base}{id}/'.format(base=Domain.PATH, id=self.id))

    @staticmethod
    def get_by_name(name):
        byNameNode = RegistryNode('{base}{name}/'.format(base=Domain.PATH_BY_NAME, name=name))
        if byNameNode.exists():
            id = byNameNode.get_property('id')
            if id:
                return Domain(id)
        return None

    @staticmethod
    def get_by_name_or_create(name):
        ins = Domain.get_by_name(name)
        if ins:
            return ins

        # We get an ID
        id = uuid.uuid1()

        # Create the domain node
        domain = Domain(id)
        domain.node.check()
        domain.node.set_property('name', name)

        # And register its ID
        byNameNode = RegistryNode('{base}{name}/'.format(base=Domain.PATH_BY_NAME, name=name)).check()
        byNameNode.set_property('id', str(id))

        return domain

    def get_name(self):
        return self.node.get_property('name')

    def get_id(self):
        return self.node.get_name()

    def delete(self, for_real=False):
        # We delete the name reference
        byNameNode = RegistryNode('{base}{name}/'.format(base=Domain.PATH_BY_NAME, name=self.get_name()))
        byNameNode.delete(for_real)

        # And then node itself
        self.node.delete(for_real)


class User:
    PATH = '/user/'
    PATH_BY_NAME = PATH + 'by-name/'

    def __init__(self, id):
        self.id = id
        self.node = RegistryNode('{base}{id}/'.format(base=User.PATH, id=self.id))

    @staticmethod
    def get_by_name(name):
        byNameNode = RegistryNode('{base}{name}/'.format(base=User.PATH_BY_NAME, name=name))
        if byNameNode.exists():
            id = byNameNode.get_property('id')
            if id:
                return User(id)
        return None

    @staticmethod
    def get_by_name_or_create(name):
        ins = User.get_by_name(name)
        if ins:
            return ins

        # We get an ID
        id = uuid.uuid1()

        # Create the user node
        user = User(id)
        user.node.check()
        user.node.set_property('name', name)

        # And register its ID
        byNameNode = RegistryNode('{base}{name}/'.format(base=User.PATH_BY_NAME, name=name)).check()
        byNameNode.set_property('id', str(id))

        # And register the corresponding domain name
        domain = Domain.get_by_name_or_create('user_'+name)
        user.set_domain(domain)

        return user

    def get_name(self):
        return self.node.get_property('name')

    def get_domain(self):
        return Domain(self.node.get_property('domain'))

    def set_domain(self, domain):
        self.node.set_property('domain', domain.get_id())

    def delete(self, for_real=False):
        # We delete the name reference
        byNameNode = RegistryNode('{base}{name}/'.format(base=User.PATH_BY_NAME, name=self.get_name()))
        byNameNode.delete(for_real)

        # And then node itself
        self.node.delete(for_real)


class TimeSeries:
    def __init__(self, id):
        pass

    DTD_SECS_DELTA = (datetime.datetime(*time.gmtime(0)[0:3]) - datetime.datetime(1582, 10, 15)).days * 86400

    @staticmethod
    def uuid1_to_date(u):
        """Return a datetime.datetime object that represents the timestamp portion of a uuid1.

        Parameters:
        u -- a type 1 uuid.UUID value

        Example usage:

        print uuid1_to_ts(uuid.uuid1())
        """
        secs_uuid1 = u.time / 1e7
        secs_epoch = secs_uuid1 - TimeSeries.DTD_SECS_DELTA
        return datetime.datetime.fromtimestamp(secs_epoch)

    @staticmethod
    def time_to_date(date):
        if type(date) is uuid.UUID:
            date = TimeSeries.uuid1_to_date(date)
        return date.strftime("%Y-%m-%d")

    @staticmethod
    def save(id, type, time, data):
        date = TimeSeries.time_to_date(time)
        session.execute(
            "insert into timeseries (id, type, date, time, data) values (%s, %s, %s, %s, %s);",
            [id, type, date, time, data]
        )

        if type:
            session.execute(
                "insert into timeseries (id, date, time, data) values (%s, %s, %s, %s);",
                [id + "!" + type, date, time, data]
            )

        session.execute_async(
            "insert into timeseries_index (id, type, date) values (%s, %s, %s);",
            [id, type, date]
        )

