
#include "sqlite_db.h"

#include <chrono>
#include <functional>
#include <iostream>
#include <map>
#include <random>
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
  None,
  Thread,
  System,
  Enabled,
  Tag,       // multiple
  Boundary,  // multiple
  Subdomain, // multiple
  ExecOn,    // multiple
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

class VecStore : public Storage
{
public:
  virtual void add(int obj_id, const std::vector<Attribute> & attribs) override
  {
    if (obj_id < _system.size())
      throw std::runtime_error("object with id " + std::to_string(obj_id) + " already added");

    _system.push_back("");
    _thread.push_back(-1);
    _enabled.push_back(true);
    _tags.push_back({});
    _boundaries.push_back({});
    _subdomains.push_back({});
    _execute_ons.push_back({});

    for (auto & attrib : attribs)
    {
      switch (attrib.id)
      {
        case AttributeId::Thread:
          _thread.back() = attrib.value;
          break;
        case AttributeId::System:
          _system.back() = attrib.strvalue;
          break;
        case AttributeId::Enabled:
          _enabled.back() = attrib.value;
          break;
        case AttributeId::Boundary:
          _boundaries.back().push_back(attrib.value);
          break;
        case AttributeId::Subdomain:
          _subdomains.back().push_back(attrib.value);
          break;
        case AttributeId::ExecOn:
          _execute_ons.back().push_back(attrib.value);
          break;
        case AttributeId::Tag:
          _tags.back().push_back(attrib.strvalue);
          break;
        default:
          throw std::runtime_error("unknown AttributeId " + std::to_string(static_cast<int>(attrib.id)));
      }
    }
  }

  virtual std::vector<int> query(const std::vector<Attribute> & conds) override
  {
    std::vector<int> objs;
    for (int i = 0; i < _system.size(); i++)
    {
      bool passes = true;
      for (auto & cond : conds)
      {
        switch (cond.id)
        {
          case AttributeId::Thread:
            if (cond.value != _thread[i])
              passes = false;
            break;
          case AttributeId::System:
            if (cond.strvalue != _system[i])
              passes = false;
            break;
          case AttributeId::Enabled:
            if (cond.value != _enabled[i])
              passes = false;
            break;
          case AttributeId::Boundary:
            passes = false;
            for (auto val : _boundaries[i])
              if (cond.value == val)
              {
                passes = true;
                break;
              }
            break;
          case AttributeId::Subdomain:
            passes = false;
            for (auto val : _subdomains[i])
              if (cond.value == val)
              {
                passes = true;
                break;
              }
            break;
          case AttributeId::ExecOn:
            passes = false;
            for (auto val : _execute_ons[i])
              if (cond.value == val)
              {
                passes = true;
                break;
              }
            break;
          case AttributeId::Tag:
            passes = false;
            for (auto val : _tags[i])
              if (cond.strvalue == val)
              {
                passes = true;
                break;
              }
            break;
          default:
            throw std::runtime_error("unknown AttributeId " + std::to_string(static_cast<int>(cond.id)));
        }
        if (!passes)
          break;
      }
      if (passes)
        objs.push_back(i);
    }
    return objs;
  }

  virtual void set(int obj_id, const Attribute & attrib) override
  {
    if (obj_id >= _system.size())
      throw std::runtime_error("no object with id " + std::to_string(obj_id));
    throw std::runtime_error("not implemented");
  }

private:
  std::vector<std::string> _system;
  std::vector<int> _thread;
  std::vector<bool> _enabled;
  std::vector<std::vector<std::string>> _tags;
  std::vector<std::vector<int>> _boundaries;
  std::vector<std::vector<int>> _subdomains;
  std::vector<std::vector<int>> _execute_ons;
};

class SqlStore : public Storage
{
public:
  SqlStore()
    : Storage(), _db(":memory:"), _in_transaction(false)
  {
    _db.Execute("CREATE TABLE objects (id INTEGER PRIMARY KEY, system TEXT, thread INTEGER, enabled INTEGER);");
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

  ~SqlStore()
  {
    auto s1 = _db.Prepare("PRAGMA PAGE_SIZE;");
    s1->Step();
    int pagesize = s1->GetInt(0);

    auto s2 = _db.Prepare("PRAGMA PAGE_COUNT;");
    s2->Step();
    int pagecount = s2->GetInt(0);

    std::cout << "Sqlite db size: " << pagecount * pagesize / 1000 << " kB (page_size=" << pagesize << ", pagecount=" << pagecount << ")\n";
  };

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
    for (auto & attrib : attribs)
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

      _db.Execute("CREATE INDEX IF NOT EXISTS idx_subdomain ON subdomains (subdomain, id);");
      _db.Execute("CREATE INDEX IF NOT EXISTS idx_boundary ON boundaries (boundary, id);");
      _db.Execute("CREATE INDEX IF NOT EXISTS idx_tag ON tags (tag, id);");
      _db.Execute("CREATE INDEX IF NOT EXISTS idx_execute_on ON execute_ons (execute_on, id);");
      _db.Execute("CREATE INDEX IF NOT EXISTS idx_objects ON objects (system, thread, enabled, id);");
      _db.Execute("CREATE INDEX IF NOT EXISTS idx2_subdomain ON subdomains (id, subdomain);");
      _db.Execute("CREATE INDEX IF NOT EXISTS idx2_boundary ON boundaries (id, boundary);");
      _db.Execute("CREATE INDEX IF NOT EXISTS idx2_tag ON tags (id, tag);");
      _db.Execute("CREATE INDEX IF NOT EXISTS idx2_execute_on ON execute_ons (id, execute_on);");
      _db.Execute("CREATE INDEX IF NOT EXISTS idx2_objects ON objects (id, system, thread, enabled);");
      _db.Execute("ANALYZE;");
    }

    std::string joins = "SELECT DISTINCT objects.id FROM objects";
    std::string tail;
    std::vector<std::function<void(SqlStatement::Ptr &)>> bindings;
    int joincount = 1;
    int tailcount = 0;
    for (int i = 0; i < conds.size(); i++)
    {
      auto & cond = conds[i];

      switch (cond.id)
      {
        case AttributeId::Thread:
          tail += " AND objects.thread=?";
          bindings.push_back([joincount, tailcount, cond](SqlStatement::Ptr & stmt) { stmt->BindInt(joincount+tailcount, cond.value); });
          tailcount++;
          break;
        case AttributeId::System:
          tail += " AND objects.system=?";
          bindings.push_back([joincount, tailcount, cond](SqlStatement::Ptr & stmt) { stmt->BindText(joincount+tailcount, cond.strvalue.c_str()); });
          tailcount++;
          break;
        case AttributeId::Enabled:
          tail += " AND objects.enabled=?";
          bindings.push_back([joincount, tailcount, cond](SqlStatement::Ptr & stmt) { stmt->BindInt(joincount+tailcount, cond.value); });
          tailcount++;
          break;
        case AttributeId::Boundary:
          joins += " JOIN boundaries AS b" + std::to_string(i) + " ON objects.id=b" + std::to_string(i) + ".id AND b" + std::to_string(i) + ".boundary=?";
          bindings.push_back([joincount, cond](SqlStatement::Ptr & stmt) { stmt->BindInt(joincount, cond.value); });
          joincount++;
          break;
        case AttributeId::Subdomain:
          joins += " JOIN subdomains AS s" + std::to_string(i) + " ON objects.id=s" + std::to_string(i) + ".id AND s" + std::to_string(i) + ".subdomain=?";
          bindings.push_back([joincount, cond](SqlStatement::Ptr & stmt) { stmt->BindInt(joincount, cond.value); });
          joincount++;
          break;
        case AttributeId::ExecOn:
          joins += " JOIN execute_ons AS e" + std::to_string(i) + " ON objects.id=e" + std::to_string(i) + ".id AND e" + std::to_string(i) + ".execute_on=?";
          bindings.push_back([joincount, cond](SqlStatement::Ptr & stmt) { stmt->BindInt(joincount, cond.value); });
          joincount++;
          break;
        case AttributeId::Tag:
          joins += " JOIN tags AS t" + std::to_string(i) + " ON objects.id=t" + std::to_string(i) + ".id AND t" + std::to_string(i) + ".tag=?";
          bindings.push_back([joincount, cond](SqlStatement::Ptr & stmt) { stmt->BindText(joincount, cond.strvalue.c_str()); });
          joincount++;
          break;
        default:
          throw std::runtime_error("unknown AttributeId " + std::to_string(static_cast<int>(cond.id)));
      }
    }

    if (tail.size() > 0)
      tail = " WHERE " + tail.substr(4, std::string::npos);

    std::string sql = joins + tail + ";";
    std::cout << "  sql: " << sql << "\n";
    auto stmt = _db.Prepare(sql);
    for (auto & func : bindings)
      func(stmt);

    std::vector<int> objs;
    while (stmt->Step())
      objs.push_back(stmt->GetInt(0));
    std::cout << "  nresults=" << objs.size() << "\n";

    //auto plan = _db.Prepare("EXPLAIN QUERY PLAN " + sql + ";");
    //std::cout << "query plan:\n";
    //while (plan->Step())
    //{
    //  int n = 0;
    //  char * s = plan->GetText(3, &n);
    //  std::string text(s, n);
    //  std::cout << "    " << plan->GetInt(0) << "|" << plan->GetInt(1) << "|" << plan->GetInt(2) << "|   " << text << "\n";
    //}

    return objs;
  }

  virtual void set(int obj_id, const Storage::Attribute & attrib) override
  {
    throw std::runtime_error("not implemented");
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
  Warehouse(Storage & s)
    : _store(s){};

  void addObject(std::unique_ptr<Object> obj)
  {
    for (int i = 0; i < _query_dirty.size(); i++)
      _query_dirty[i] = true;

    std::vector<Storage::Attribute> attribs;
    attribs.push_back({AttributeId::System, 0, obj->system});
    attribs.push_back({AttributeId::Thread, obj->thread, ""});
    attribs.push_back({AttributeId::Enabled, obj->enabled, ""});
    for (auto & tag : obj->tags)
      attribs.push_back({AttributeId::Tag, 0, tag});
    for (auto & sub : obj->subdomains)
      attribs.push_back({AttributeId::Subdomain, sub, ""});
    for (auto & bound : obj->boundaries)
      attribs.push_back({AttributeId::Boundary, bound, ""});
    for (auto & on : obj->execute_ons)
      attribs.push_back({AttributeId::ExecOn, on, ""});

    _objects.push_back(std::move(obj));
    _store.add(_objects.size() - 1, attribs);
  }

  // prepares a query and returns an associated query_id (i.e. for use with the query function.
  int prepare(const std::vector<Storage::Attribute> & conds)
  {
    auto obj_ids = _store.query(conds);

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
  //////////////////// create objects /////////////////////////////
  int nboundaries = 1000;
  int nsubdomains = 10000;
  int nthreads = 10;
  int nsystems = 50;
  int nexecons = 10;
  int ntags = 10;
  int nobjects = 1000000;

  int boundaries_per_object = 1000;
  int subdomains_per_object = 10000;
  int tags_per_object = 3;
  int execs_per_object = 5;

  int seed = 7;
  std::mt19937 gen(seed);
  std::uniform_int_distribution<> distbound(1, nboundaries);
  std::uniform_int_distribution<> distsubdomain(1, nsubdomains);
  std::uniform_int_distribution<> disttag(1, ntags);
  std::uniform_int_distribution<> distexecon(1, nexecons);
  std::uniform_int_distribution<> distthread(1, nthreads);
  std::uniform_int_distribution<> distsystem(1, nsystems);
  // mean number of subdomains and boundaries per object is 10 and 3 respectively
  std::geometric_distribution<> distsubdomains_per_object(1.0 / 10.0);
  std::geometric_distribution<> distboundaries_per_object(1.0 / 3.0);

  std::vector<std::string> tags;
  for (int i = 0; i < ntags; i++)
    tags.push_back(std::to_string(i));

  std::vector<std::string> systems;
  for (int i = 0; i < nsystems; i++)
    systems.push_back(std::to_string(i));

  int tagtally = 0;
  int boundtally = 0;
  int subdomaintally = 0;
  int exectally = 0;

  std::vector<std::unique_ptr<Object>> objects;
  for (int i = 0; i < nobjects; i++)
  {
    if (i % 1000 == 0)
      std::cout << "created " << i << " objects\n";
    auto object = new Object();
    objects.emplace_back(object);
    auto & obj = *objects[i];
    obj.thread = distthread(gen);
    obj.enabled = true;
    obj.system = systems[distsystem(gen) - 1];

    for (int j = 0; j < tags_per_object; j++)
      obj.tags.push_back(tags[disttag(gen) - 1]);
    for (int j = 0; j < distboundaries_per_object(gen); j++)
      obj.boundaries.push_back(distbound(gen));
    for (int j = 0; j < distsubdomains_per_object(gen); j++)
      obj.subdomains.push_back(distsubdomain(gen));
    for (int j = 0; j < execs_per_object; j++)
      obj.execute_ons.push_back(distexecon(gen));

    tagtally += obj.tags.size();
    boundtally += obj.boundaries.size();
    subdomaintally += obj.subdomains.size();
    exectally += obj.execute_ons.size();
  }

  ////////////// create queries /////////////////////
  int nqueries = 1000;
  int conds_per_query = 10;
  std::uniform_int_distribution<> distbool(0, 1);
  std::uniform_int_distribution<> distconds(0, 2);
  std::vector<std::vector<Storage::Attribute>> queries;
  for (int i = 0; i < nqueries; i++)
  {
    std::vector<Storage::Attribute> conds;
    if (distbool(gen))
      conds.push_back({AttributeId::Thread, distthread(gen), ""});
    if (distbool(gen))
      conds.push_back({AttributeId::System, 0, systems[distsystem(gen) - 1]});

    int n = distconds(gen);
    for (int j = 0; j < n; j++)
      conds.push_back({AttributeId::Tag, 0, tags[disttag(gen) - 1]});
    n = distconds(gen);
    for (int j = 0; j < n; j++)
      conds.push_back({AttributeId::Subdomain, disttag(gen), ""});
    n = distconds(gen);
    for (int j = 0; j < n; j++)
      conds.push_back({AttributeId::Boundary, disttag(gen), ""});
    n = distconds(gen);
    for (int j = 0; j < n; j++)
      conds.push_back({AttributeId::ExecOn, disttag(gen), ""});
    queries.push_back(conds);
  }

  //////////////////// insert objects ////////////////////////////////
  SqlStore sstore;
  VecStore vstore;
  Warehouse w(vstore);

  auto start = std::chrono::steady_clock::now();
  for (auto & obj : objects)
    w.addObject(std::move(obj));
  auto end = std::chrono::steady_clock::now();

  auto diff = end - start;
  std::cout << "insert time: " << std::chrono::duration_cast<std::chrono::milliseconds>(diff).count() << " ms\n";

  ////////////////// query objects (with cache) ////////////////////////

  // 1st run (cold cache)
  start = std::chrono::steady_clock::now();
  int countn = 0;
  std::vector<int> queryids;
  int qcount = 0;
  for (auto & q : queries)
  {
    qcount++;
    std::cout << "running query " << qcount << "\n";
    queryids.push_back(w.prepare(q));
    auto & v = w.query(queryids.back());
    countn += v.size();
  }
  end = std::chrono::steady_clock::now();
  diff = end - start;
  std::cout << "query 1st time: " << std::chrono::duration_cast<std::chrono::milliseconds>(diff).count() << " ms (" << countn << " total results)\n";

  // 2nd run with cache
  countn = 0;
  start = std::chrono::steady_clock::now();
  for (auto & q : queryids)
  {
    auto & v = w.query(q);
    countn += v.size();
  }
  end = std::chrono::steady_clock::now();
  diff = end - start;
  std::cout << "query 2nd time: " << std::chrono::duration_cast<std::chrono::milliseconds>(diff).count() << " ms (" << countn << " total results)\n";

  std::cout << "total stored items:\n";
  std::cout << "    tags = " << tagtally << "\n";
  std::cout << "    subdomains = " << subdomaintally << "\n";
  std::cout << "    boundaries = " << boundtally << "\n";
  std::cout << "    execute_ons = " << exectally << "\n";

  return 0;
}

