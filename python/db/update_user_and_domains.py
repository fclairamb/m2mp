import models


def convert_domains():
    rows = models.session.execute('select name, id from Domain;')
    for row in rows:
        print('')
        print('Domain {id} : {name}'.format(id=row.id, name=row.name))
        node = models.RegistryNode('{base}{id}/'.format(base=models.Domain.PATH, id=row.id)).create()
        node.set_property('name', row.name)

        nodeByName = models.RegistryNode('{base}{name}/'.format(base=models.Domain.PATH_BY_NAME, name=row.name)).create()
        print('  nodeByName: {node}'.format(node=nodeByName))

        nodeByName.set_property('id', str(row.id))

def convert_users():
    rows = models.session.execute('select name, id from User;')
    for row in rows:
        print('')
        print('User {id} : {name}'.format(id=row.id, name=row.name))
        node = models.RegistryNode('{base}{id}/'.format(base=models.User.PATH, id=row.id)).create()
        node.set_property('name', row.name)

        nodeByName = models.RegistryNode('{base}{name}/'.format(base=models.User.PATH_BY_NAME, name=row.name)).create()
        print('  nodeByName: {node}'.format(node=nodeByName))

        nodeByName.set_property('id', str(row.id))

models.set_session()

convert_domains()
