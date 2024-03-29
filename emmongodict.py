from connbase import *

def myclassmethod(method):
    return classmethod(method)

class EmMongoDict(object):
    '''
    '''
    datatype = None
    db_info = {
        'db':'EmMongoDict',
        'collection':'EmMongoDict',
    }

    indexes = {
        #'name':{'unique':True, 'sparse':True}
    }
    coll = None

    def __init__(self, doc=None, spec=None, path=None, 
                        db=None, collection=None):
        '''
        '''
        self.db_info = dict(self.db_info)
        if collection is not None:
            self.db_info['collection'] = collection
        if db is not None:
            self.db_info['db'] = db
        self.spec = spec
        self.path = path
        self.coll = None
        if not self.spec:
            if doc is None:
                doc = dict()
            self.spec = {'_id':self.new_doc(doc=doc)}
        elif doc:
            self.update(doc)

    @myclassmethod
    @auto_coll_do
    def new_doc(cls, doc={}):
        if cls.datatype:
            return cls.datatype()['_id']
        return cls.coll.insert(doc_or_docs=doc)


    @auto_coll_do
    def is_exist(self):
        '''test whether spec is in db'''
        ret = self.coll.find_one(spec_or_id=self.spec, fields={'_id':1})
        if not ret:
            return False
        return True

    @auto_coll_do
    def ensure_exist(self):
        '''return: True, if exist before
                   False, if not exist before
        '''
        if self.is_exist() is False:
            self.coll.insert(doc_or_docs=self.spec)
            return False
        return True

    @auto_coll_do
    def mongo_update(self, *args, **kwargs):
        '''same as update in pymongo'''
        return self.coll.update(*args, **kwargs)

    @auto_coll_do
    def subdict(self, subpath):
        path = subpath if not self.path else self.path+'.'+subpath
        return self.__class__(spec=self.spec, path=path)

    @auto_coll_do
    def sublist(self, subpath):
        from emmongolist import EmMongoList
        path = subpath if not self.path else self.path+'.'+subpath
        return EmMongoList(spec=self.spec, path=path)

    @myclassmethod
    @auto_coll_do
    def init_collection(cls):
        cls.drop()
        cls.ensure_index()


    @myclassmethod
    @auto_coll_do
    def drop(cls):
        cls.coll.drop()

    @myclassmethod
    @auto_coll_do
    def ensure_index(cls):
        for ek, ev in cls.indexes.items():
            info = dict(cls.db_info)
            info.update(ev)
            cls.coll.ensure_index(key_or_list=ek)

    @auto_coll_do
    def __getitem__(self, key):
        toget = key
        if self.path is not None:
            toget = self.path+'.'+toget
        ret = self.coll.find_one(spec_or_id=self.spec, fields={toget:1})
        return get_dict_property(ret, toget)

    @auto_coll_do
    def get_propertys(self, keylist):
        togets = keylist
        if self.path is not None:
            togets = dict((self.path+'.'+each, 1) for each in togets)
        else:
            togets = dict((each, 1) for each in togets)
        ret = self.coll.find_one(spec_or_id=self.spec, fields={toget:1})
        return [get_dict_property(ret, each) for each in togets]

    @auto_coll_do
    def __setitem__(self, key, value):
        if self.path is not None:
            key = self.path+'.'+key
        return self.coll.update(spec=self.spec, document={'$set':{key:value}})

    @auto_coll_do
    def delete_propertys(self, keylist):
        if self.path is not None:
            keylist = [self.path+'.'+each for each in keylist]
        return coll_delete_keys(self.coll, keys=keylist, spec=self.spec)

    @auto_coll_do
    def __delitem__(self, key):
        return self.delete_propertys(key)

    @auto_coll_do
    def inc(self, key, step=1):
        return coll_inc(self.coll, spec=self.spec, key=key, step=step)

    @auto_coll_do
    def dec(self, key, step=1):
        return coll_inc(self.coll, spec=self.spec, key=key, step=-step)

    @auto_coll_do
    def incb(self, key, step=1):
        '''similar to i++'''
        return coll_incb(self.coll, spec=self.spec, key=key, step=step)

    @auto_coll_do
    def inca(self, key, step=1):
        '''similar to ++i'''
        return coll_inca(self.coll, spec=self.spec, key=key, step=step)

    @auto_coll_do
    def decb(self, key, step=1):
        '''similar to i--'''
        return coll_incb(self.coll, spec=self.spec, key=key, step=-step)

    @auto_coll_do
    def deca(self, key, step=1):
        '''similar to --i'''
        return coll_inca(self.coll, spec=self.spec, key=key, step=-step)

    @auto_coll_do
    def update(self, other):
        doc = dict(other)
        if self.path is not None:
            doc = dict([(self.path+'.'+ek, ev) for ek, ev in doc.items()])
        return self.coll.update(spec=self.spec, document={'$set':doc})

    def pop(self, key):
        ret = self[key]
        del self[key]
        return ret

    @auto_coll_do
    def load_doc(self):
        if self.path is None:
            ret = self.coll.find_one(spec_or_id=self.spec)
            return ret
        ret = self.coll.find_one(spec_or_id=self.spec, fields={self.path:1})
        return get_dict_property(ret, self.path)

    load_all = load_doc

    @auto_coll_do
    def set_doc(self, newdoc):
        if not self.path:
            return self.coll.update(spec=self.spec,
                    document=newdoc)
        return self.coll.update(spec=self.spec,
                    document={'$set':{self.path:newdoc}})

    set_all = set_doc

    @myclassmethod
    @auto_coll_do
    def load_docs(cls, spec_key=None, spec_values=None):
        if spec_key is None and spec_values is None:
            return cls.coll.find()
        return cls.coll.find(spec={spec_key:{'$in':spec_values}})
        
    def __iter__(self):
        doc = self.load_doc()
        for each in doc.keys():
            yield each

    def items(self):
        doc = self.load_doc()
        return doc.items()

    def keys(self):
        doc = self.load_doc()
        return doc.keys()

    def values(self):
        doc = self.load_doc()
        return doc.values()

    def __len__(self):
        doc = self.load_doc()
        if doc is not None:
            return len(doc)
        return 0

    def __contains__(self, item):
        return self[item] is not None

    @auto_coll_do
    def remove(self):
        '''remove the dict from db'''
        if self.path is not None:
            return coll_delete_keys(self.coll, 
                    keys=(self.path,), spec=self.spec)
        return self.coll.remove(spec_or_id=self.spec)

    @auto_coll_do
    def rename(self, key, newkey):
        '''rename key by newkey'''
        if self.path is not None:
            key = self.path+'.'+key
            newkey=self.path+'.'+newkey
        return self.coll.update(spec=self.spec, 
                    document={'$rename':{key:newkey}})
