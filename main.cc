
#include "sqlite_db.h"

#include <iostream>
#include <map>
#include <string>
#include <vector>

class Object
{
public:
    int thread = 0;
    std::string system;
    bool enabled = true;

    std::vector<int> boundaries;
    std::vector<int> subdomains;
    std::vector<std::string> tags;
    std::vector<int> execute_ons;
};

// attributes include:
//
//     * tag (multiple) few - 3ish
//     * system - order 50
//     * execute_on (multiple) 10 max
//     * thread_id - order 10
//     * boundary_id (multiple) 1000 per mesh, 1000 per object (use "all/any" optimization)
//     * subdomain_id (multiple) 10000 per mesh, 1000 per object (use "all/any" optimization)
//     * enabled
enum class AttributeId
{
  Thread,
  System,
  Enabled,
  Tag,      // multiple
  Boundary, // multiple
  Subdomain, // multiple
  ExecOn, // multiple
};

class Storage
{
public:
  struct Attribute
  {
    AttributeId id;
    int value;
    std::string strvalue;
    bool operator==(const Attribute & other) const
    {
      return id == other.id && value == other.value && strvalue == other.strvalue;
    }
  };

  virtual void add(int obj_id, const std::vector<Attribute> & attribs) = 0;
  virtual std::vector<int> query(const std::vector<Attribute> & conds) = 0;
  virtual void set(int obj_id, const Attribute & attrib) = 0;
};

class SqlStore : public Storage
{
public:
  SqlStore()
    : Storage(), _db(":memory:"), _in_transaction(false)
  {
    _db.Execute("CREATE TABLE objects (id INTEGER, system TEXT, thread INTEGER, enabled INTEGER);");
    _db.Execute("CREATE TABLE subdomains (id INTEGER, subdomain INTEGER);");
    _db.Execute("CREATE TABLE boundaries (id INTEGER, boundary INTEGER);");
    _db.Execute("CREATE TABLE execute_ons (id INTEGER, execute_on INTEGER);");
    _db.Execute("CREATE TABLE tags (id INTEGER, tag TEXT);");
    _tblmain = _db.Prepare("INSERT INTO objects (id, system, thread, enabled) VALUES (?,?,?,?);");
    _tbltag = _db.Prepare("INSERT INTO tags (id, tag) VALUES (?,?);");
    _tblbound = _db.Prepare("INSERT INTO boundaries (id, boundary) VALUES (?,?);");
    _tblsubdomain = _db.Prepare("INSERT INTO subdomains (id, subdomain) VALUES (?,?);");
    _tblexecons = _db.Prepare("INSERT INTO execute_ons (id, execute_on) VALUES (?,?);");
  }

  virtual void add(int obj_id, const std::vector<Storage::Attribute> & attribs) override
  {
    if (!_in_transaction)
    {
      _in_transaction = true;
      _db.Execute("BEGIN TRANSACTION;");
    }

    bool enabled = true;
    int thread = -1;
    std::string system;
    std::vector<std::string> tags;
    std::vector<int> bounds;
    std::vector<int> subdomains;
    std::vector<int> execons;
    for (auto& attrib : attribs)
    {
        switch (attrib.id)
        {
        case AttributeId::Thread:
            thread = attrib.value;
            break;
        case AttributeId::System:
            system = attrib.strvalue;
            break;
        case AttributeId::Enabled:
            enabled = attrib.value;
            break;
        case AttributeId::Boundary:
            _tblbound->BindInt(1, obj_id);
            _tblbound->BindInt(2, attrib.value);
            _tblbound->Exec();
            break;
        case AttributeId::Subdomain:
            _tblsubdomain->BindInt(1, obj_id);
            _tblsubdomain->BindInt(2, attrib.value);
            _tblsubdomain->Exec();
            break;
        case AttributeId::ExecOn:
            _tblexecons->BindInt(1, obj_id);
            _tblexecons->BindInt(2, attrib.value);
            _tblexecons->Exec();
            break;
        case AttributeId::Tag:
            _tbltag->BindInt(1, obj_id);
            _tbltag->BindText(2, attrib.strvalue.c_str());
            _tbltag->Exec();
            break;
        default:
            throw std::runtime_error("unknown AttributeId " + std::to_string(static_cast<int>(attrib.id)));
        }
    }

    _tblmain->BindInt(1, obj_id);
    _tblmain->BindText(2, system.c_str());
    _tblmain->BindInt(3, thread);
    _tblmain->BindInt(4, enabled);
    _tblmain->Exec();
  }

  virtual std::vector<int> query(const std::vector<Storage::Attribute> & conds) override
  {
    if (_in_transaction)
    {
      _in_transaction = false;
      _db.Execute("END TRANSACTION;");
    }

    return {};
  }

  virtual void set(int obj_id, const Storage::Attribute & attrib) override
  {
  }

private:
  SqliteDb _db;
  bool _in_transaction;
  SqlStatement::Ptr _tblmain;
  SqlStatement::Ptr _tbltag;
  SqlStatement::Ptr _tblbound;
  SqlStatement::Ptr _tblsubdomain;
  SqlStatement::Ptr _tblexecons;
};

class Warehouse
{
public:
  Warehouse(Storage & s, bool use_cache = true)
    : _store(s), _use_cache(use_cache){};

  void addObject(std::unique_ptr<Object> obj)
  {
    for (int i = 0; i < _query_dirty.size(); i++)
      _query_dirty[i] = true;

    std::vector<Storage::Attribute> attribs;
    attribs.push_back({AttributeId::System, 0, obj->system});
    attribs.push_back({AttributeId::Thread, obj->thread, ""});
    attribs.push_back({AttributeId::Enabled, obj->enabled, ""});
    for (auto& tag : obj->tags)
        attribs.push_back({AttributeId::Tag, 0, tag});
    for (auto& sub : obj->subdomains)
        attribs.push_back({AttributeId::Subdomain, sub, ""});
    for (auto& bound : obj->boundaries)
        attribs.push_back({AttributeId::Boundary, bound, ""});
    for (auto& on : obj->execute_ons)
        attribs.push_back({AttributeId::ExecOn, on, ""});

    _objects.push_back(std::move(obj));
    _store.add(_objects.size() - 1, attribs);
  }

  // prepares a query and returns an associated query_id (i.e. for use with the query function.
  int prepare(const std::vector<Storage::Attribute> & conds)
  {
    auto obj_ids = _store.query(conds);

    // see if this query matches an existing cached one.
    if (_use_cache && _query_cache.size() < 100)
    {
      for (int i = 0; i < _query_cache.size(); i++)
      {
        if (_query_cache[i] == conds)
          return i;
      }
    }

    _query_dirty.push_back(true);
    _obj_cache.push_back({});
    _query_cache.push_back(conds);

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
  Storage & _store;
  std::vector<std::unique_ptr<Object>> _objects;

  bool _use_cache;
  std::vector<std::vector<Object *>> _obj_cache;
  std::vector<std::vector<Storage::Attribute>> _query_cache;
  std::vector<bool> _query_dirty;
};

// not needed?
int
tagid(const std::string & s)
{
  static std::map<std::string, int> ids;
  if (ids.count(s) == 0)
    ids[s] = ids.size();
  return ids[s];
}

int
main(int argc, char ** argv)
{
  int nboundaries = 1000;
  int nsubdomains = 10000;
  int nthreads = 10;
  int nsystems = 50;
  int nobjects = 1000000;

  int boundaries_per_object = 1000;
  int subdomains_per_object = 10000;
  int tags_per_object = 5;
  int execs_per_object = 10;

  SqlStore store;
  Warehouse w(store);

  return 0;
}

