import redis
import json
import hashlib
import encodings

def hasher(val, hash_type='md5'):
    if hash_type == 'md5':
        h = hashlib.md5()
    elif hash_type == 'sha1':
        h = hashlib.sha1()
    elif hash_type == 'sha256':
        h = hashlib.sha256()
    elif hash_type == 'sha512':
        h = hashlib.sha512()
    else:
        raise ValueError('invalid hash_type ' + str(hash_type))
    val = val.encode()
    h.update(val)
    return h.digest()


def type_check(val, desired_type):
    if type(desired_type) == list:
        if type(val) in desired_type:
            return True
        else:
            return False
    elif type(val) == desired_type:
        return True
    else:
        return False

def to_type(val, desired_type):
    if desired_type == 'str':
        return str(val)
    elif desired_type == 'int':
        return int(val)
    elif desired_type == 'float':
        return float(val)
    elif desired_type == 'bool':
        return bool(val)
    else:
        raise TypeError('attempted to convert to type with invalid destination type')

class Database(object):

    def __init__(self, host='localhost', port=6379, db=0):
        self.red = redis.Redis(host=host, port=port, db=db)

    def insert(self, key, val):
        try:
            self.red.set(key,val)
            return True
        except:
            return False

    def read(self, key):
        return self.red.get(key)


class RedObj(object):

    def __init__(self, db=None, **kwargs):
        self.registery = {}
        self.set_db(db)
        for k,v in kwargs.items():
            if k != 'db':
                self.add_attr(k,v)

    def add_attr(self, name, value):
        self.check_db()
        red_val = self.to_red_val(value)
        key = hasher(name)
        self.__setattr__(name, value)
        self.registery[name] = {'key':key, 'type':type(value)}
        self.db.insert(key, red_val)

    def refresh_all(self):
        self.check_db()
        for attr_name in self.registery.keys():
            vals = self.registery[attr_name]
            key = vals['key']
            red_val = self.db.read(key)
            real_val = self.from_red_val(attr_name, red_val)
            self.__setattr__(attr_name, real_val)

    def set_all(self):
        self.check_db()
        for attr_name in self.registery.keys():
            vals = self.registery[attr_name]
            key = vals['key']
            red_val = self.to_red_val(self.__getattribute__(attr_name))
            self.db.insert(key, red_val)

    def from_red_val(self, name, value):
        dest_type = self.registery[name]['type']
        value = value.decode()
        if dest_type in [str, int, float, bool]:
            return dest_type(value)
        elif dest_type == list:
            l = []
            for i in value.split('|'):
                t,v = i.split('~')
                l.append(to_type(v,t))
            return l
        elif dest_type == dict:
            return json.loads(value)
        else:
            raise TypeError('registry type for attr is invalid')

    def to_red_val(self, value):
        if type_check(value, [str, int, float, bool]):
            return str(value)
        elif type_check(value, dict):
            return json.dumps(value)
        elif type_check(value, list):
            l_strs = []
            for i in value:
                if type(i) == str:
                    l_strs.append(f'str~{i}')
                elif type(i) == int:
                    l_strs.append(f'int~{i}')
                elif type(i) == float:
                    l_strs.append(f'float~{i}')
                elif type(i) == bool:
                    l_strs.append(f'bool~{i}')
                else:
                    raise TypeError('invalid type in list, all list items must be str, int, float, bool')
            return '|'.join(l_strs)
        else:
            raise TypeError('value is of invalid type. type must be str, int, float, bool, dict, list')

    def check_db(self):
        if type_check(self.db, Database):
            pass
        else:
            raise ValueError('Database is not set')

    def set_db(self, db):
        if db == None:
            self.db = None
        else:
            if type_check(db, Database):
                self.db = db
            else:
                raise TypeError('db must be type Database')


if __name__ == '__main__':
    print('Testing Database')
    db = Database()
    red_o = RedObj(db=db, test_1='1', test_2=2, test_3=3.0, test_4=True, test_5=['1',2,3.0], test_6={'test':'1'})
    print(red_o.test_1, red_o.test_2, red_o.test_3, red_o.test_4, red_o.test_5, red_o.test_6)
    red_o.refresh_all()
    print(red_o.test_1, red_o.test_2, red_o.test_3, red_o.test_4, red_o.test_5, red_o.test_6)
    red_o.set_all()
    for key in red_o.registery.keys():
        v = red_o.registery[key]
        print(db.read(v['key']))
    