/*-------------------------------------------------------------------------
 *
 * sqlite Foreign Data Wrapper for PostgreSQL
 *
 * Copyright (c) 2013-2016 Guillaume Lelarge
 *
 * This software is released under the PostgreSQL Licence
 *
 * Author: Guillaume Lelarge <guillaume@lelarge.info>
 *
 * IDENTIFICATION
 *        sqlite_fdw/src/sqlite_fdw.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/reloptions.h"
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "optimizer/pathnode.h"
#include "optimizer/planmain.h"
#include "optimizer/restrictinfo.h"

#include "funcapi.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_foreign_server.h"
#include "catalog/pg_foreign_table.h"
#include "commands/defrem.h"
#include "commands/explain.h"
#include "utils/builtins.h"
#include "utils/formatting.h"
#include "utils/rel.h"
#include "utils/lsyscache.h"

#include <sqlite3.h>
#include <sys/stat.h>

PG_MODULE_MAGIC;

/*
 * Default values
 */
/* ----
 * This value is taken from sqlite
   (without stats, sqlite defaults to 1 million tuples for a table)
 */
#define DEFAULT_ESTIMATED_LINES 1000000

/*
 * SQL functions
 */
extern Datum sqlite_fdw_handler(PG_FUNCTION_ARGS);
extern Datum sqlite_fdw_validator(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(sqlite_fdw_handler);
PG_FUNCTION_INFO_V1(sqlite_fdw_validator);


/* callback functions */
#if (PG_VERSION_NUM >= 90200)
static void sqliteGetForeignRelSize(PlannerInfo *root,
						   RelOptInfo *baserel,
						   Oid foreigntableid);

static void sqliteGetForeignPaths(PlannerInfo *root,
						 RelOptInfo *baserel,
						 Oid foreigntableid);

static ForeignScan *sqliteGetForeignPlan(PlannerInfo *root,
						RelOptInfo *baserel,
						Oid foreigntableid,
						ForeignPath *best_path,
						List *tlist,
#if (PG_VERSION_NUM >= 90500)
						List *scan_clauses,
						Plan *outer_plan);
#else
						List *scan_clauses);
#endif
#else
static FdwPlan *sqlitePlanForeignScan(Oid foreigntableid, PlannerInfo *root, RelOptInfo *baserel);
#endif

static void sqliteBeginForeignScan(ForeignScanState *node,
						  int eflags);

static TupleTableSlot *sqliteIterateForeignScan(ForeignScanState *node);

static void sqliteReScanForeignScan(ForeignScanState *node);

static void sqliteEndForeignScan(ForeignScanState *node);

#if (PG_VERSION_NUM >= 90300)
static void sqliteAddForeignUpdateTargets(Query *parsetree,
								 RangeTblEntry *target_rte,
								 Relation target_relation);

static List *sqlitePlanForeignModify(PlannerInfo *root,
						   ModifyTable *plan,
						   Index resultRelation,
						   int subplan_index);

static void sqliteBeginForeignModify(ModifyTableState *mtstate,
							ResultRelInfo *rinfo,
							List *fdw_private,
							int subplan_index,
							int eflags);

static TupleTableSlot *sqliteExecForeignInsert(EState *estate,
						   ResultRelInfo *rinfo,
						   TupleTableSlot *slot,
						   TupleTableSlot *planSlot);

static TupleTableSlot *sqliteExecForeignUpdate(EState *estate,
						   ResultRelInfo *rinfo,
						   TupleTableSlot *slot,
						   TupleTableSlot *planSlot);

static TupleTableSlot *sqliteExecForeignDelete(EState *estate,
						   ResultRelInfo *rinfo,
						   TupleTableSlot *slot,
						   TupleTableSlot *planSlot);

static void sqliteEndForeignModify(EState *estate,
						  ResultRelInfo *rinfo);
#endif

static void sqliteExplainForeignScan(ForeignScanState *node,
							struct ExplainState *es);

#if (PG_VERSION_NUM >= 90300)
static void sqliteExplainForeignModify(ModifyTableState *mtstate,
							  ResultRelInfo *rinfo,
							  List *fdw_private,
							  int subplan_index,
							  struct ExplainState *es);
#endif

#if (PG_VERSION_NUM >= 90200)
static bool sqliteAnalyzeForeignTable(Relation relation,
							 AcquireSampleRowsFunc *func,
							 BlockNumber *totalpages);
#endif

#if (PG_VERSION_NUM >= 90500)
static List *sqliteImportForeignSchema(ImportForeignSchemaStmt *stmt,
							 Oid serverOid);
#endif

/*
 * Helper functions
 */
static void sqliteOpen(char *filename, sqlite3 **db);
static void sqlitePrepare(sqlite3 *db, char *query, sqlite3_stmt **result, const char **pzTail);
static bool sqliteIsValidOption(const char *option, Oid context);
static void sqliteGetOptions(Oid foreigntableid, char **database, char **table);
static int GetEstimatedRows(Oid foreigntableid);
static bool file_exists(const char *name);
#if (PG_VERSION_NUM >= 90500)
/* only used for IMPORT FOREIGN SCHEMA */
static void sqliteTranslateType(StringInfo str, char *typname);
#endif


/*
 * structures used by the FDW
 *
 * These next two are not actually used by sqlite, but something like this
 * will be needed by anything more complicated that does actual work.
 *
 */

/*
 * Describes the valid options for objects that use this wrapper.
 */
struct SQLiteFdwOption
{
	const char	*optname;
	Oid		optcontext;	/* Oid of catalog in which option may appear */
};

/*
 * Describes the valid options for objects that use this wrapper.
 */
static struct SQLiteFdwOption valid_options[] =
{

	/* Connection options */
	{ "database",  ForeignServerRelationId },

	/* Table options */
	{ "table",     ForeignTableRelationId },

	/* Sentinel */
	{ NULL,			InvalidOid }
};

/*
 * This is what will be set and stashed away in fdw_private and fetched
 * for subsequent routines.
 */
typedef struct
{
	char	   *foo;
	int			bar;
}	sqliteFdwPlanState;

/*
 * FDW-specific information for ForeignScanState.fdw_state.
 */

typedef struct SQLiteFdwExecutionState
{
	sqlite3       *conn;
	sqlite3_stmt  *result;
	char          *query;
} SQLiteFdwExecutionState;


Datum
sqlite_fdw_handler(PG_FUNCTION_ARGS)
{
	FdwRoutine *fdwroutine = makeNode(FdwRoutine);

	elog(DEBUG1,"entering function %s",__func__);

	/* assign the handlers for the FDW */

	/* these are required */
#if (PG_VERSION_NUM >= 90200)
	fdwroutine->GetForeignRelSize = sqliteGetForeignRelSize;
	fdwroutine->GetForeignPaths = sqliteGetForeignPaths;
	fdwroutine->GetForeignPlan = sqliteGetForeignPlan;
#else
	fdwroutine->PlanForeignScan = sqlitePlanForeignScan;
#endif
	fdwroutine->BeginForeignScan = sqliteBeginForeignScan;
	fdwroutine->IterateForeignScan = sqliteIterateForeignScan;
	fdwroutine->ReScanForeignScan = sqliteReScanForeignScan;
	fdwroutine->EndForeignScan = sqliteEndForeignScan;

	/* remainder are optional - use NULL if not required */
	/* support for insert / update / delete */
#if (PG_VERSION_NUM >= 90300)
	fdwroutine->AddForeignUpdateTargets = sqliteAddForeignUpdateTargets;
	fdwroutine->PlanForeignModify = sqlitePlanForeignModify;
	fdwroutine->BeginForeignModify = sqliteBeginForeignModify;
	fdwroutine->ExecForeignInsert = sqliteExecForeignInsert;
	fdwroutine->ExecForeignUpdate = sqliteExecForeignUpdate;
	fdwroutine->ExecForeignDelete = sqliteExecForeignDelete;
	fdwroutine->EndForeignModify = sqliteEndForeignModify;
#endif

	/* support for EXPLAIN */
	fdwroutine->ExplainForeignScan = sqliteExplainForeignScan;
#if (PG_VERSION_NUM >= 90300)
	fdwroutine->ExplainForeignModify = sqliteExplainForeignModify;
#endif

#if (PG_VERSION_NUM >= 90200)
	/* support for ANALYSE */
	fdwroutine->AnalyzeForeignTable = sqliteAnalyzeForeignTable;
#endif

#if (PG_VERSION_NUM >= 90500)
	/* support for IMPORT FOREIGN SCHEMA */
	fdwroutine->ImportForeignSchema = sqliteImportForeignSchema;
#endif

	PG_RETURN_POINTER(fdwroutine);
}

Datum
sqlite_fdw_validator(PG_FUNCTION_ARGS)
{
	List      *options_list = untransformRelOptions(PG_GETARG_DATUM(0));
	Oid       catalog = PG_GETARG_OID(1);
	char      *svr_database = NULL;
	char      *svr_table = NULL;
	ListCell  *cell;

	elog(DEBUG1,"entering function %s",__func__);

	/*
	 * Check that only options supported by sqlite_fdw,
	 * and allowed for the current object type, are given.
	 */
	foreach(cell, options_list)
	{
		DefElem	   *def = (DefElem *) lfirst(cell);

		if (!sqliteIsValidOption(def->defname, catalog))
		{
			struct SQLiteFdwOption *opt;
			StringInfoData buf;

			/*
			 * Unknown option specified, complain about it. Provide a hint
			 * with list of valid options for the object.
			 */
			initStringInfo(&buf);
			for (opt = valid_options; opt->optname; opt++)
			{
				if (catalog == opt->optcontext)
					appendStringInfo(&buf, "%s%s", (buf.len > 0) ? ", " : "",
							 opt->optname);
			}

			ereport(ERROR,
				(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
				errmsg("invalid option \"%s\"", def->defname),
				errhint("Valid options in this context are: %s", buf.len ? buf.data : "<none>")
				));
		}

		if (strcmp(def->defname, "database") == 0)
		{
			if (svr_database)
				ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					errmsg("redundant options: database (%s)", defGetString(def))
					));
			if (!file_exists(defGetString(def)))
				ereport(ERROR,
					(errcode_for_file_access(),
					errmsg("could not access file \"%s\"", defGetString(def))
					));

			svr_database = defGetString(def);
		}
		else if (strcmp(def->defname, "table") == 0)
		{
			if (svr_table)
				ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					errmsg("redundant options: table (%s)", defGetString(def))
					));

			svr_table = defGetString(def);
		}
	}

	/* Check we have the options we need to proceed */
	if (catalog == ForeignServerRelationId && !svr_database)
		ereport(ERROR,
			(errcode(ERRCODE_SYNTAX_ERROR),
			errmsg("The database name must be specified")
			));

	PG_RETURN_VOID();
}

/*
 * Open the given sqlite3 file, and throw an error if the file couldn't be
 * opened
 */
static void
sqliteOpen(char *filename, sqlite3 **db)
{
	if (sqlite3_open(filename, db) != SQLITE_OK) {
		ereport(ERROR,
			(errcode(ERRCODE_FDW_OUT_OF_MEMORY),
			errmsg("Can't open sqlite database %s: %s", filename, sqlite3_errmsg(*db))
			));
		sqlite3_close(*db);
	}
}
/*
 * Prepare the given query. If case of error, close the db and throw an error
 */
static void
sqlitePrepare(sqlite3 *db, char *query, sqlite3_stmt **result,
		const char **pzTail)
{
	int rc;

	/* Execute the query */
	rc = sqlite3_prepare(db, query, -1, result, pzTail);
	if (rc != SQLITE_OK)
	{
		ereport(ERROR,
			(errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
			errmsg("SQL error during prepare: %s", sqlite3_errmsg(db))
			));

		sqlite3_close(db);
	}
}

/*
 * Check if the provided option is one of the valid options.
 * context is the Oid of the catalog holding the object the option is for.
 */
static bool
sqliteIsValidOption(const char *option, Oid context)
{
	struct SQLiteFdwOption *opt;

	for (opt = valid_options; opt->optname; opt++)
	{
		if (context == opt->optcontext && strcmp(opt->optname, option) == 0)
			return true;
	}

	return false;
}

/*
 * Fetch the options for a mysql_fdw foreign table.
 */
static void
sqliteGetOptions(Oid foreigntableid, char **database, char **table)
{
	ForeignTable   *f_table;
	ForeignServer  *f_server;
	List           *options;
	ListCell       *lc;

	/*
	 * Extract options from FDW objects.
	 */
	f_table = GetForeignTable(foreigntableid);
	f_server = GetForeignServer(f_table->serverid);

	options = NIL;
	options = list_concat(options, f_table->options);
	options = list_concat(options, f_server->options);

	/* Loop through the options */
	foreach(lc, options)
	{
		DefElem *def = (DefElem *) lfirst(lc);

		if (strcmp(def->defname, "database") == 0)
		{
			*database = defGetString(def);
		}

		if (strcmp(def->defname, "table") == 0)
		{
			*table = defGetString(def);
		}
	}

	if (!*table)
	{
		*table = get_rel_name(foreigntableid);
	}

	/* Check we have the options we need to proceed */
	if (!*database || !*table)
		ereport(ERROR,
			(errcode(ERRCODE_SYNTAX_ERROR),
			errmsg("a database and a table must be specified")
			));
}


#if (PG_VERSION_NUM >= 90200)
static void
sqliteGetForeignRelSize(PlannerInfo *root,
						   RelOptInfo *baserel,
						   Oid foreigntableid)
{
	/*
	 * Obtain relation size estimates for a foreign table. This is called at
	 * the beginning of planning for a query that scans a foreign table. root
	 * is the planner's global information about the query; baserel is the
	 * planner's information about this table; and foreigntableid is the
	 * pg_class OID of the foreign table. (foreigntableid could be obtained
	 * from the planner data structures, but it's passed explicitly to save
	 * effort.)
	 *
	 * This function should update baserel->rows to be the expected number of
	 * rows returned by the table scan, after accounting for the filtering
	 * done by the restriction quals. The initial value of baserel->rows is
	 * just a constant default estimate, which should be replaced if at all
	 * possible. The function may also choose to update baserel->width if it
	 * can compute a better estimate of the average result row width.
	 */
	sqliteFdwPlanState *fdw_private;

	elog(DEBUG1,"entering function %s",__func__);

	baserel->rows = 0;

	fdw_private = palloc0(sizeof(sqliteFdwPlanState));
	baserel->fdw_private = (void *) fdw_private;

	/* initialize required state in fdw_private */
	baserel->rows = GetEstimatedRows(foreigntableid);
}

static void
sqliteGetForeignPaths(PlannerInfo *root,
						 RelOptInfo *baserel,
						 Oid foreigntableid)
{
	/*
	 * Create possible access paths for a scan on a foreign table. This is
	 * called during query planning. The parameters are the same as for
	 * GetForeignRelSize, which has already been called.
	 *
	 * This function must generate at least one access path (ForeignPath node)
	 * for a scan on the foreign table and must call add_path to add each such
	 * path to baserel->pathlist. It's recommended to use
	 * create_foreignscan_path to build the ForeignPath nodes. The function
	 * can generate multiple access paths, e.g., a path which has valid
	 * pathkeys to represent a pre-sorted result. Each access path must
	 * contain cost estimates, and can contain any FDW-private information
	 * that is needed to identify the specific scan method intended.
	 */

	/*
	 * sqliteFdwPlanState *fdw_private = baserel->fdw_private;
	 */

	Cost		startup_cost,
				total_cost;

	elog(DEBUG1,"entering function %s",__func__);

	startup_cost = 0;
	total_cost = startup_cost + baserel->rows;

	/* Create a ForeignPath node and add it as only possible path */
	add_path(baserel, (Path *)
			 create_foreignscan_path(root, baserel,
#if (PG_VERSION_NUM >= 90600)
									 NULL,		/* default pathtarget */
#endif
									 baserel->rows,
									 startup_cost,
									 total_cost,
									 NIL,		/* no pathkeys */
									 NULL,		/* no outer rel either */
#if (PG_VERSION_NUM >= 90500)
									 NULL,		/* no extra plan */
#endif
									 NIL));		/* no fdw_private data */
}



static ForeignScan *
sqliteGetForeignPlan(PlannerInfo *root,
						RelOptInfo *baserel,
						Oid foreigntableid,
						ForeignPath *best_path,
						List *tlist,
#if (PG_VERSION_NUM >= 90500)
						List *scan_clauses,
						Plan *outer_plan)
#else
						List *scan_clauses)
#endif
{
	/*
	 * Create a ForeignScan plan node from the selected foreign access path.
	 * This is called at the end of query planning. The parameters are as for
	 * GetForeignRelSize, plus the selected ForeignPath (previously produced
	 * by GetForeignPaths), the target list to be emitted by the plan node,
	 * and the restriction clauses to be enforced by the plan node.
	 *
	 * This function must create and return a ForeignScan plan node; it's
	 * recommended to use make_foreignscan to build the ForeignScan node.
	 *
	 */

	Index		scan_relid = baserel->relid;

	/*
	 * We have no native ability to evaluate restriction clauses, so we just
	 * put all the scan_clauses into the plan node's qual list for the
	 * executor to check. So all we have to do here is strip RestrictInfo
	 * nodes from the clauses and ignore pseudoconstants (which will be
	 * handled elsewhere).
	 */

	elog(DEBUG1,"entering function %s",__func__);

	scan_clauses = extract_actual_clauses(scan_clauses, false);

	/* Create the ForeignScan node */
	return make_foreignscan(tlist,
							scan_clauses,
							scan_relid,
							NIL,	/* no expressions to evaluate */
#if (PG_VERSION_NUM >= 90500)
							NIL,	/* no private state */
							NIL,	/* no custom tlist */
							NIL,	/* no recheck quals */
							NULL);	/* no extra plan */
#else
							NIL);	/* no private state */
#endif

}
#else
static FdwPlan *
sqlitePlanForeignScan(Oid foreigntableid, PlannerInfo *root, RelOptInfo *baserel)
{
	FdwPlan		*fdwplan;

	/* Construct FdwPlan with cost estimates. */
	fdwplan = makeNode(FdwPlan);


	baserel->rows = GetEstimatedRows(foreigntableid);
	/* TODO: find a way to estimate the average row size */
	/*baserel->width = ?; */
	baserel->tuples = baserel->rows;

	fdwplan->startup_cost = 10;
	fdwplan->total_cost = baserel->rows + fdwplan->startup_cost;
	fdwplan->fdw_private = NIL;	/* not used */

	return fdwplan;
}
#endif


static void
sqliteBeginForeignScan(ForeignScanState *node,
						  int eflags)
{
	/*
	 * Begin executing a foreign scan. This is called during executor startup.
	 * It should perform any initialization needed before the scan can start,
	 * but not start executing the actual scan (that should be done upon the
	 * first call to IterateForeignScan). The ForeignScanState node has
	 * already been created, but its fdw_state field is still NULL.
	 * Information about the table to scan is accessible through the
	 * ForeignScanState node (in particular, from the underlying ForeignScan
	 * plan node, which contains any FDW-private information provided by
	 * GetForeignPlan). eflags contains flag bits describing the executor's
	 * operating mode for this plan node.
	 *
	 * Note that when (eflags & EXEC_FLAG_EXPLAIN_ONLY) is true, this function
	 * should not perform any externally-visible actions; it should only do
	 * the minimum required to make the node state valid for
	 * ExplainForeignScan and EndForeignScan.
	 *
	 */
	sqlite3                  *db;
	SQLiteFdwExecutionState  *festate;
	char                     *svr_database = NULL;
	char                     *svr_table = NULL;
	char                     *query;
	size_t                   len;

	elog(DEBUG1,"entering function %s",__func__);

	/* Fetch options  */
	sqliteGetOptions(RelationGetRelid(node->ss.ss_currentRelation), &svr_database, &svr_table);

	/* Connect to the server */
	sqliteOpen(svr_database, &db);

	/* Build the query */
	len = strlen(svr_table) + 15;
	query = (char *)palloc(len);
	snprintf(query, len, "SELECT * FROM %s", svr_table);

	/* Stash away the state info we have already */
	festate = (SQLiteFdwExecutionState *) palloc(sizeof(SQLiteFdwExecutionState));
	node->fdw_state = (void *) festate;
	festate->conn = db;
	festate->result = NULL;
	festate->query = query;
}


static TupleTableSlot *
sqliteIterateForeignScan(ForeignScanState *node)
{
	/*
	 * Fetch one row from the foreign source, returning it in a tuple table
	 * slot (the node's ScanTupleSlot should be used for this purpose). Return
	 * NULL if no more rows are available. The tuple table slot infrastructure
	 * allows either a physical or virtual tuple to be returned; in most cases
	 * the latter choice is preferable from a performance standpoint. Note
	 * that this is called in a short-lived memory context that will be reset
	 * between invocations. Create a memory context in BeginForeignScan if you
	 * need longer-lived storage, or use the es_query_cxt of the node's
	 * EState.
	 *
	 * The rows returned must match the column signature of the foreign table
	 * being scanned. If you choose to optimize away fetching columns that are
	 * not needed, you should insert nulls in those column positions.
	 *
	 * Note that PostgreSQL's executor doesn't care whether the rows returned
	 * violate any NOT NULL constraints that were defined on the foreign table
	 * columns — but the planner does care, and may optimize queries
	 * incorrectly if NULL values are present in a column declared not to
	 * contain them. If a NULL value is encountered when the user has declared
	 * that none should be present, it may be appropriate to raise an error
	 * (just as you would need to do in the case of a data type mismatch).
	 */

	char        **values;
	HeapTuple   tuple;
	int         x;
	const char  *pzTail;

	SQLiteFdwExecutionState *festate = (SQLiteFdwExecutionState *) node->fdw_state;
	TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;

	elog(DEBUG1,"entering function %s",__func__);

	/* Execute the query, if required */
	if (!festate->result)
	{
		sqlitePrepare(festate->conn, festate->query, &festate->result, &pzTail);
	}

	ExecClearTuple(slot);

	/* get the next record, if any, and fill in the slot */
	if (sqlite3_step(festate->result) == SQLITE_ROW)
	{
		/* Build the tuple */
		values = (char **) palloc(sizeof(char *) * sqlite3_column_count(festate->result));

		for (x = 0; x < sqlite3_column_count(festate->result); x++)
		{
			values[x] = (char *) sqlite3_column_text(festate->result, x);
		}

		tuple = BuildTupleFromCStrings(TupleDescGetAttInMetadata(node->ss.ss_currentRelation->rd_att), values);
		ExecStoreTuple(tuple, slot, InvalidBuffer, false);
	}

	/* then return the slot */
	return slot;
}


static void
sqliteReScanForeignScan(ForeignScanState *node)
{
	/*
	 * Restart the scan from the beginning. Note that any parameters the scan
	 * depends on may have changed value, so the new scan does not necessarily
	 * return exactly the same rows.
	 */

	elog(DEBUG1,"entering function %s",__func__);

}


static void
sqliteEndForeignScan(ForeignScanState *node)
{
	/*
	 * End the scan and release resources. It is normally not important to
	 * release palloc'd memory, but for example open files and connections to
	 * remote servers should be cleaned up.
	 */

	SQLiteFdwExecutionState *festate = (SQLiteFdwExecutionState *) node->fdw_state;

	elog(DEBUG1,"entering function %s",__func__);

	if (festate->result)
	{
		sqlite3_finalize(festate->result);
		festate->result = NULL;
	}

	if (festate->conn)
	{
		sqlite3_close(festate->conn);
		festate->conn = NULL;
	}

	if (festate->query)
	{
		pfree(festate->query);
		festate->query = 0;
	}

}


#if (PG_VERSION_NUM >= 90300)
static void
sqliteAddForeignUpdateTargets(Query *parsetree,
								 RangeTblEntry *target_rte,
								 Relation target_relation)
{
	/*
	 * UPDATE and DELETE operations are performed against rows previously
	 * fetched by the table-scanning functions. The FDW may need extra
	 * information, such as a row ID or the values of primary-key columns, to
	 * ensure that it can identify the exact row to update or delete. To
	 * support that, this function can add extra hidden, or "junk", target
	 * columns to the list of columns that are to be retrieved from the
	 * foreign table during an UPDATE or DELETE.
	 *
	 * To do that, add TargetEntry items to parsetree->targetList, containing
	 * expressions for the extra values to be fetched. Each such entry must be
	 * marked resjunk = true, and must have a distinct resname that will
	 * identify it at execution time. Avoid using names matching ctidN or
	 * wholerowN, as the core system can generate junk columns of these names.
	 *
	 * This function is called in the rewriter, not the planner, so the
	 * information available is a bit different from that available to the
	 * planning routines. parsetree is the parse tree for the UPDATE or DELETE
	 * command, while target_rte and target_relation describe the target
	 * foreign table.
	 *
	 * If the AddForeignUpdateTargets pointer is set to NULL, no extra target
	 * expressions are added. (This will make it impossible to implement
	 * DELETE operations, though UPDATE may still be feasible if the FDW
	 * relies on an unchanging primary key to identify rows.)
	 */

	elog(DEBUG1,"entering function %s",__func__);

}


static List *
sqlitePlanForeignModify(PlannerInfo *root,
						   ModifyTable *plan,
						   Index resultRelation,
						   int subplan_index)
{
	/*
	 * Perform any additional planning actions needed for an insert, update,
	 * or delete on a foreign table. This function generates the FDW-private
	 * information that will be attached to the ModifyTable plan node that
	 * performs the update action. This private information must have the form
	 * of a List, and will be delivered to BeginForeignModify during the
	 * execution stage.
	 *
	 * root is the planner's global information about the query. plan is the
	 * ModifyTable plan node, which is complete except for the fdwPrivLists
	 * field. resultRelation identifies the target foreign table by its
	 * rangetable index. subplan_index identifies which target of the
	 * ModifyTable plan node this is, counting from zero; use this if you want
	 * to index into plan->plans or other substructure of the plan node.
	 *
	 * If the PlanForeignModify pointer is set to NULL, no additional
	 * plan-time actions are taken, and the fdw_private list delivered to
	 * BeginForeignModify will be NIL.
	 */

	elog(DEBUG1,"entering function %s",__func__);


	return NULL;
}


static void
sqliteBeginForeignModify(ModifyTableState *mtstate,
							ResultRelInfo *rinfo,
							List *fdw_private,
							int subplan_index,
							int eflags)
{
	/*
	 * Begin executing a foreign table modification operation. This routine is
	 * called during executor startup. It should perform any initialization
	 * needed prior to the actual table modifications. Subsequently,
	 * ExecForeignInsert, ExecForeignUpdate or ExecForeignDelete will be
	 * called for each tuple to be inserted, updated, or deleted.
	 *
	 * mtstate is the overall state of the ModifyTable plan node being
	 * executed; global data about the plan and execution state is available
	 * via this structure. rinfo is the ResultRelInfo struct describing the
	 * target foreign table. (The ri_FdwState field of ResultRelInfo is
	 * available for the FDW to store any private state it needs for this
	 * operation.) fdw_private contains the private data generated by
	 * PlanForeignModify, if any. subplan_index identifies which target of the
	 * ModifyTable plan node this is. eflags contains flag bits describing the
	 * executor's operating mode for this plan node.
	 *
	 * Note that when (eflags & EXEC_FLAG_EXPLAIN_ONLY) is true, this function
	 * should not perform any externally-visible actions; it should only do
	 * the minimum required to make the node state valid for
	 * ExplainForeignModify and EndForeignModify.
	 *
	 * If the BeginForeignModify pointer is set to NULL, no action is taken
	 * during executor startup.
	 */

	elog(DEBUG1,"entering function %s",__func__);

}


static TupleTableSlot *
sqliteExecForeignInsert(EState *estate,
						   ResultRelInfo *rinfo,
						   TupleTableSlot *slot,
						   TupleTableSlot *planSlot)
{
	/*
	 * Insert one tuple into the foreign table. estate is global execution
	 * state for the query. rinfo is the ResultRelInfo struct describing the
	 * target foreign table. slot contains the tuple to be inserted; it will
	 * match the rowtype definition of the foreign table. planSlot contains
	 * the tuple that was generated by the ModifyTable plan node's subplan; it
	 * differs from slot in possibly containing additional "junk" columns.
	 * (The planSlot is typically of little interest for INSERT cases, but is
	 * provided for completeness.)
	 *
	 * The return value is either a slot containing the data that was actually
	 * inserted (this might differ from the data supplied, for example as a
	 * result of trigger actions), or NULL if no row was actually inserted
	 * (again, typically as a result of triggers). The passed-in slot can be
	 * re-used for this purpose.
	 *
	 * The data in the returned slot is used only if the INSERT query has a
	 * RETURNING clause. Hence, the FDW could choose to optimize away
	 * returning some or all columns depending on the contents of the
	 * RETURNING clause. However, some slot must be returned to indicate
	 * success, or the query's reported rowcount will be wrong.
	 *
	 * If the ExecForeignInsert pointer is set to NULL, attempts to insert
	 * into the foreign table will fail with an error message.
	 *
	 */

	elog(DEBUG1,"entering function %s",__func__);

	return slot;
}


static TupleTableSlot *
sqliteExecForeignUpdate(EState *estate,
						   ResultRelInfo *rinfo,
						   TupleTableSlot *slot,
						   TupleTableSlot *planSlot)
{
	/*
	 * Update one tuple in the foreign table. estate is global execution state
	 * for the query. rinfo is the ResultRelInfo struct describing the target
	 * foreign table. slot contains the new data for the tuple; it will match
	 * the rowtype definition of the foreign table. planSlot contains the
	 * tuple that was generated by the ModifyTable plan node's subplan; it
	 * differs from slot in possibly containing additional "junk" columns. In
	 * particular, any junk columns that were requested by
	 * AddForeignUpdateTargets will be available from this slot.
	 *
	 * The return value is either a slot containing the row as it was actually
	 * updated (this might differ from the data supplied, for example as a
	 * result of trigger actions), or NULL if no row was actually updated
	 * (again, typically as a result of triggers). The passed-in slot can be
	 * re-used for this purpose.
	 *
	 * The data in the returned slot is used only if the UPDATE query has a
	 * RETURNING clause. Hence, the FDW could choose to optimize away
	 * returning some or all columns depending on the contents of the
	 * RETURNING clause. However, some slot must be returned to indicate
	 * success, or the query's reported rowcount will be wrong.
	 *
	 * If the ExecForeignUpdate pointer is set to NULL, attempts to update the
	 * foreign table will fail with an error message.
	 *
	 */

	elog(DEBUG1,"entering function %s",__func__);

	return slot;
}


static TupleTableSlot *
sqliteExecForeignDelete(EState *estate,
						   ResultRelInfo *rinfo,
						   TupleTableSlot *slot,
						   TupleTableSlot *planSlot)
{
	/*
	 * Delete one tuple from the foreign table. estate is global execution
	 * state for the query. rinfo is the ResultRelInfo struct describing the
	 * target foreign table. slot contains nothing useful upon call, but can
	 * be used to hold the returned tuple. planSlot contains the tuple that
	 * was generated by the ModifyTable plan node's subplan; in particular, it
	 * will carry any junk columns that were requested by
	 * AddForeignUpdateTargets. The junk column(s) must be used to identify
	 * the tuple to be deleted.
	 *
	 * The return value is either a slot containing the row that was deleted,
	 * or NULL if no row was deleted (typically as a result of triggers). The
	 * passed-in slot can be used to hold the tuple to be returned.
	 *
	 * The data in the returned slot is used only if the DELETE query has a
	 * RETURNING clause. Hence, the FDW could choose to optimize away
	 * returning some or all columns depending on the contents of the
	 * RETURNING clause. However, some slot must be returned to indicate
	 * success, or the query's reported rowcount will be wrong.
	 *
	 * If the ExecForeignDelete pointer is set to NULL, attempts to delete
	 * from the foreign table will fail with an error message.
	 */

	elog(DEBUG1,"entering function %s",__func__);

	return slot;
}


static void
sqliteEndForeignModify(EState *estate,
						  ResultRelInfo *rinfo)
{
	/*
	 * End the table update and release resources. It is normally not
	 * important to release palloc'd memory, but for example open files and
	 * connections to remote servers should be cleaned up.
	 *
	 * If the EndForeignModify pointer is set to NULL, no action is taken
	 * during executor shutdown.
	 */

	elog(DEBUG1,"entering function %s",__func__);

}
#endif


static void
sqliteExplainForeignScan(ForeignScanState *node,
							struct ExplainState *es)
{
	/*
	 * Print additional EXPLAIN output for a foreign table scan. This function
	 * can call ExplainPropertyText and related functions to add fields to the
	 * EXPLAIN output. The flag fields in es can be used to determine what to
	 * print, and the state of the ForeignScanState node can be inspected to
	 * provide run-time statistics in the EXPLAIN ANALYZE case.
	 *
	 * If the ExplainForeignScan pointer is set to NULL, no additional
	 * information is printed during EXPLAIN.
	 */
	sqlite3					   *db;
	sqlite3_stmt			   *result;
	char					   *svr_database = NULL;
	char					   *svr_table = NULL;
	char					   *query;
	size_t						len;
	const char				   *pzTail;
	SQLiteFdwExecutionState	   *festate = (SQLiteFdwExecutionState *) node->fdw_state;

	elog(DEBUG1,"entering function %s",__func__);

	/* Show the query (only if VERBOSE) */
	if (es->verbose)
	{
		/* show query */
		ExplainPropertyText("sqlite query", festate->query, es);
	}

	/* Fetch options  */
	sqliteGetOptions(RelationGetRelid(node->ss.ss_currentRelation), &svr_database, &svr_table);

	/* Connect to the server */
	sqliteOpen(svr_database, &db);

	/* Build the query */
	len = strlen(festate->query) + 20;
	query = (char *)palloc(len);
	snprintf(query, len, "EXPLAIN QUERY PLAN %s", festate->query);

    /* Execute the query */
	sqlitePrepare(db, query, &result, &pzTail);

	/* get the next record, if any, and fill in the slot */
	while (sqlite3_step(result) == SQLITE_ROW)
	{
		/*
		 * I don't keep the three first columns;
		   it could be a good idea to add that later
		 */
		/*
		 * for (x = 0; x < sqlite3_column_count(festate->result); x++)
		 * {
		 */
			ExplainPropertyText("sqlite plan", (char*)sqlite3_column_text(result, 3), es);
		/* } */
	}

	/* Free the query results */
	sqlite3_finalize(result);

	/* Close temporary connection */
	sqlite3_close(db);

}


#if (PG_VERSION_NUM >= 90300)
static void
sqliteExplainForeignModify(ModifyTableState *mtstate,
							  ResultRelInfo *rinfo,
							  List *fdw_private,
							  int subplan_index,
							  struct ExplainState *es)
{
	/*
	 * Print additional EXPLAIN output for a foreign table update. This
	 * function can call ExplainPropertyText and related functions to add
	 * fields to the EXPLAIN output. The flag fields in es can be used to
	 * determine what to print, and the state of the ModifyTableState node can
	 * be inspected to provide run-time statistics in the EXPLAIN ANALYZE
	 * case. The first four arguments are the same as for BeginForeignModify.
	 *
	 * If the ExplainForeignModify pointer is set to NULL, no additional
	 * information is printed during EXPLAIN.
	 */

	elog(DEBUG1,"entering function %s",__func__);

}
#endif


#if (PG_VERSION_NUM >= 90200)
static bool
sqliteAnalyzeForeignTable(Relation relation,
							 AcquireSampleRowsFunc *func,
							 BlockNumber *totalpages)
{
	/* ----
	 * This function is called when ANALYZE is executed on a foreign table. If
	 * the FDW can collect statistics for this foreign table, it should return
	 * true, and provide a pointer to a function that will collect sample rows
	 * from the table in func, plus the estimated size of the table in pages
	 * in totalpages. Otherwise, return false.
	 *
	 * If the FDW does not support collecting statistics for any tables, the
	 * AnalyzeForeignTable pointer can be set to NULL.
	 *
	 * If provided, the sample collection function must have the signature:
	 *
	 *	  int
	 *	  AcquireSampleRowsFunc (Relation relation, int elevel,
	 *							 HeapTuple *rows, int targrows,
	 *							 double *totalrows,
	 *							 double *totaldeadrows);
	 *
	 * A random sample of up to targrows rows should be collected from the
	 * table and stored into the caller-provided rows array. The actual number
	 * of rows collected must be returned. In addition, store estimates of the
	 * total numbers of live and dead rows in the table into the output
	 * parameters totalrows and totaldeadrows. (Set totaldeadrows to zero if
	 * the FDW does not have any concept of dead rows.)
	 * ----
	 */

	elog(DEBUG1,"entering function %s",__func__);

	return false;
}
#endif

#if (PG_VERSION_NUM >= 90500)
static List *
sqliteImportForeignSchema(ImportForeignSchemaStmt *stmt,
							 Oid serverOid)
{
	sqlite3		   *volatile db = NULL;
	sqlite3_stmt   *volatile tbls = NULL;
	ForeignServer  *f_server;
	ListCell	   *lc;
	char		   *svr_database = NULL;
	StringInfoData	query_tbl;
	const char	   *pzTail;
	List		   *commands = NIL;
	bool			import_default = false;
	bool			import_not_null = true;

	elog(DEBUG1,"entering function %s",__func__);

	/*
	 * The only legit sqlite schema are temp and main (or name of an attached
	 * database, which can't happen here).  Authorize only legit "main" schema,
	 * and "public" just in case.
	 */
	if (strcmp(stmt->remote_schema, "public") != 0 &&
		strcmp(stmt->remote_schema, "main") != 0)
	{
		ereport(ERROR,
			(errcode(ERRCODE_FDW_SCHEMA_NOT_FOUND),
			errmsg("Foreign schema \"%s\" is invalid", stmt->remote_schema)
			));
	}

	/* Parse statement options */
	foreach(lc, stmt->options)
	{
		DefElem    *def = (DefElem *) lfirst(lc);

		if (strcmp(def->defname, "import_default") == 0)
			import_default = defGetBoolean(def);
		else if (strcmp(def->defname, "import_not_null") == 0)
			import_not_null = defGetBoolean(def);
		else
			ereport(ERROR,
					(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
					  errmsg("invalid option \"%s\"", def->defname)));
	}

	/* get the db filename */
	f_server = GetForeignServerByName(stmt->server_name, false);
	foreach(lc, f_server->options)
	{
		DefElem *def = (DefElem *) lfirst(lc);

		if (strcmp(def->defname, "database") == 0)
		{
			svr_database = defGetString(def);
			break;
		}
	}

	Assert(svr_database);

	/* Connect to the server */
	sqliteOpen(svr_database, (sqlite3 **) &db);

	PG_TRY();
	{
		/* You want all tables, except system tables */
		initStringInfo(&query_tbl);
		appendStringInfo(&query_tbl, "SELECT name FROM sqlite_master WHERE type = 'table'");
		appendStringInfo(&query_tbl, " AND name NOT LIKE 'sqlite_%%'");

		/* Handle LIMIT TO / EXCEPT clauses in IMPORT FOREIGN SCHEMA statement */
		if (stmt->list_type == FDW_IMPORT_SCHEMA_LIMIT_TO ||
			stmt->list_type == FDW_IMPORT_SCHEMA_EXCEPT)
		{
			bool		first_item = true;

			appendStringInfoString(&query_tbl, " AND name ");
			if (stmt->list_type == FDW_IMPORT_SCHEMA_EXCEPT)
				appendStringInfoString(&query_tbl, "NOT ");
			appendStringInfoString(&query_tbl, "IN (");

			foreach(lc, stmt->table_list)
			{
				RangeVar *rv = (RangeVar *) lfirst(lc);

				if (first_item)
					first_item = false;
				else
					appendStringInfoString(&query_tbl, ", ");

				appendStringInfoString(&query_tbl, quote_literal_cstr(rv->relname));
			}
			appendStringInfoChar(&query_tbl, ')');
		}

		/* Iterate to all matching tables, and get their definition */
		sqlitePrepare(db, query_tbl.data, (sqlite3_stmt **) &tbls, &pzTail);
		while (sqlite3_step(tbls) == SQLITE_ROW)
		{
			sqlite3_stmt   *cols;
			char		   *tbl_name;
			char		   *query_cols;
			StringInfoData	cft_stmt;
			int				i = 0;

			tbl_name = (char *) sqlite3_column_text(tbls, 0);

			/*
			 * user-defined list of tables has been handled in main query, don't
			 * try to do the job here again
			 */

			/* start building the CFT stmt */
			initStringInfo(&cft_stmt);
			appendStringInfo(&cft_stmt, "CREATE FOREIGN TABLE %s.%s (\n",
					stmt->local_schema, quote_identifier(tbl_name));

			query_cols = palloc0(strlen(tbl_name) + 19 + 1);
			sprintf(query_cols, "PRAGMA table_info(%s)", tbl_name);

			sqlitePrepare(db, query_cols, &cols, &pzTail);
			while (sqlite3_step(cols) == SQLITE_ROW)
			{
				char   *col_name;
				char   *typ_name;
				bool	not_null;
				char   *default_val;

				col_name = (char *) sqlite3_column_text(cols, 1);
				typ_name = (char *) sqlite3_column_text(cols, 2);
				not_null = (sqlite3_column_int(cols, 3) == 1);
				default_val = (char *) sqlite3_column_text(cols, 4);

				if (i != 0)
					appendStringInfo(&cft_stmt, ",\n");

				/* table name */
				appendStringInfo(&cft_stmt, "%s ",
						quote_identifier(col_name));

				/* translated datatype */
				sqliteTranslateType(&cft_stmt, typ_name);

				if (not_null && import_not_null)
					appendStringInfo(&cft_stmt, " NOT NULL");

				if (default_val && import_default)
					appendStringInfo(&cft_stmt, " DEFAULT %s", default_val);

				i++;
			}
			appendStringInfo(&cft_stmt, "\n) SERVER %s\n"
					"OPTIONS (table '%s')",
					quote_identifier(stmt->server_name),
					quote_identifier(tbl_name));

			commands = lappend(commands,
					pstrdup(cft_stmt.data));

			/* free per-table allocated data */
			pfree(query_cols);
			pfree(cft_stmt.data);
		}

		/* Free all needed data and close connection*/
		pfree(query_tbl.data);
	}
	PG_CATCH();
	{
		if (tbls)
			sqlite3_finalize(tbls);
		if (db)
			sqlite3_close(db);

		PG_RE_THROW();
	}
	PG_END_TRY();

	sqlite3_finalize(tbls);
	sqlite3_close(db);

	return commands;
}
#endif

static int
GetEstimatedRows(Oid foreigntableid)
{
	sqlite3		   *db;
	sqlite3_stmt   *result;
	char		   *svr_database = NULL;
	char		   *svr_table = NULL;
	char		   *query;
	size_t			len;
	const char	   *pzTail;

	int rows = 0;

	/* Fetch options  */
	sqliteGetOptions(foreigntableid, &svr_database, &svr_table);

	/* Connect to the server */
	sqliteOpen(svr_database, &db);

	/* Check that the sqlite_stat1 table exists */
	sqlitePrepare(db, "SELECT 1 FROM sqlite_master WHERE name='sqlite_stat1'", &result, &pzTail);

	if (sqlite3_step(result) != SQLITE_ROW)
	{
		ereport(WARNING,
			(errcode(ERRCODE_FDW_TABLE_NOT_FOUND),
			errmsg("The sqlite3 database has not been analyzed."),
			errhint("Run ANALYZE on table \"%s\", database \"%s\".", svr_table, svr_database)
			));
		rows = 10;
	}

	/* Free the query results */
	sqlite3_finalize(result);

	if (rows == 0)
	{
		/* Build the query */
	    len = strlen(svr_table) + 60;
	    query = (char *)palloc(len);
	    snprintf(query, len, "SELECT stat FROM sqlite_stat1 WHERE tbl='%s' AND idx IS NULL", svr_table);
	    elog(LOG, "query:%s", query);

	    /* Execute the query */
		sqlitePrepare(db, query, &result, &pzTail);

		/* get the next record, if any, and fill in the slot */
		if (sqlite3_step(result) == SQLITE_ROW)
		{
			rows = sqlite3_column_int(result, 0);
		}

		/* Free the query results */
		sqlite3_finalize(result);
	}
	else
	{
		/*
		 * The sqlite database doesn't have any statistics.
		 * There's not much we can do, except using a hardcoded one.
		 * Using a foreign table option might be a better solution.
		 */
		rows = DEFAULT_ESTIMATED_LINES;
	}

	/* Close temporary connection */
	sqlite3_close(db);

	return rows;
}

static bool
file_exists(const char *name)
{
	struct stat st;

	AssertArg(name != NULL);

	if (stat(name, &st) == 0)
		return S_ISDIR(st.st_mode) ? false : true;
	else if (!(errno == ENOENT || errno == ENOTDIR || errno == EACCES))
		ereport(ERROR,
                (errcode_for_file_access(),
				 errmsg("could not access file \"%s\": %m", name)));

	return false;
}

#if (PG_VERSION_NUM >= 90500)
/*
 * Translate sqlite type name to postgres compatible one and append it to
 * given StringInfo
 */
static void
sqliteTranslateType(StringInfo str, char *typname)
{
	char *type;

	/*
	 * get lowercase typname. We use C collation since the original type name
	 * should not contain exotic character.
	 */
	type = str_tolower(typname, strlen(typname), C_COLLATION_OID);

	/* try some easy conversion, from https://www.sqlite.org/datatype3.html */
	if (strcmp(type, "tinyint") == 0)
		appendStringInfoString(str, "smallint");

	else if (strcmp(type, "mediumint") == 0)
		appendStringInfoString(str, "integer");

	else if (strcmp(type, "unsigned big int") == 0)
		appendStringInfoString(str, "bigint");

	else if (strcmp(type, "double") == 0)
		appendStringInfoString(str, "double precision");

	else if (strcmp(type, "datetime") == 0)
		appendStringInfoString(str, "timestamp");

	/* XXX try harder handling sqlite datatype */

	/* if original type is compatible, return lowercase value */
	else
		appendStringInfoString(str, type);

	pfree(type);
}
#endif
