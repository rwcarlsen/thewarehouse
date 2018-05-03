
#include <iostream>
#include <string>
#include <map>
#include <vector>

class Object { };

class Storage {
public:
  struct Attribute {
    int id;
    int value;
    bool operator == (const Attribute & other) const { return id == other.id && value == other.value;}
  };

  // attributes include:
  //
  //     * tag (multiple)
  //     * thread_id
  //     * boundary_id (multiple)
  //     * subdomain_id
  //     * enabled
  virtual void add(int obj_id, const std::vector<Attribute> attribs) = 0;
  virtual std::vector<int> query(const std::vector<Attribute>& conds) = 0;
  virtual void set(int obj_id, Attribute attrib) = 0;
};

int tagid(const std::string & s)
{
  static std::map<std::string, int> ids;
  if (ids.count(s) == 0)
    ids[s] = ids.size();
  return ids[s];
}

enum class Attributes
{
  Thread,
  Tag, // multiple
  Boundary, // multiple
  Subdomain,
  Enabled,
};

class Warehouse {
public:
  struct Attribute {
    std::string name;
    int value;
  };

  Warehouse(Storage& s, bool use_cache = true) : _store(s), _use_cache(use_cache) {};

  // attributes include:
  //
  //     * tag (multiple)
  //     * thread_id
  //     * boundary_id (multiple)
  //     * subdomain_id
  //     * enabled
  void addObject(std::unique_ptr<Object> obj, const std::vector<Attribute> & attribs)
  {
    for (int i = 0; i < _query_dirty.size(); i++)
      _query_dirty[i] = true;

    // build the id-based version of attribs (i.e. not name/string based)
    std::vector<Storage::Attribute> store_attribs;
    for (auto & attrib : attribs)
    {
      if (_attribute_ids.count(attrib.name) == 0)
        _attribute_ids[attrib.name] = _attribute_ids.size();
      store_attribs.push_back({_attribute_ids[attrib.name], attrib.value});
    }

    _objects.push_back(std::move(obj));
    _store.add(_objects.size() - 1, store_attribs);
  }

  // prepares a query and returns an associated query_id (i.e. for use with the query function.
  int prepare(const std::vector<Attribute> & conds)
  {
    // build the id-based version of conds (i.e. not name/string based)
    std::vector<Storage::Attribute> store_conds;
    for (auto & cond : conds)
      store_conds.push_back({_attribute_ids[cond.name], cond.value});
    auto obj_ids = _store.query(store_conds);

    // see if this query matches an existing cached one.
    if (_use_cache && _query_cache.size() < 100)
    {
      for (int i = 0; i < _query_cache.size(); i++)
      {
        if (_query_cache[i] == store_conds)
          return i;
      }
    }

    _query_dirty.push_back(true);
    _obj_cache.push_back({});
    _query_cache.push_back(store_conds);

    return _obj_cache.size() - 1;
  }

  const std::vector<Object *> & query(int query_id)
  {
    if (query_id >= _obj_cache.size())
      throw std::runtime_error("unknown query id");

    if (_query_dirty[query_id])
    {
      auto & vec = _obj_cache[query_id];
      vec.clear();
      for (auto & id : _store.query(_query_cache[query_id]))
        vec.push_back(_objects[id].get());
      _query_dirty[query_id] = false;
    }

    return _obj_cache[query_id];
  }

private:
  Storage& _store;
  std::vector<std::unique_ptr<Object>> _objects;

  bool _use_cache;
  std::map<std::string, int> _attribute_ids;
  std::vector<std::vector<Object*>> _obj_cache;
  std::vector<std::vector<Storage::Attribute>> _query_cache;
  std::vector<bool> _query_dirty;
};

int
main(int argc, char ** argv)
{

  return 0;
}

