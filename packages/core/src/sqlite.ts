import { Layer } from "effect";
import { SqliteClient } from "@effect/sql-sqlite-bun";
import type * as SqlClient from "effect/sql/SqlClient";

/**
 * Convenience `SqlClient` for a Bun SQLite file (WAL enabled by default). It is
 * one driver choice among many: pass any other `SqlClient` layer to
 * `layerDurableWorkflowEngine` and `layerSqlFlowPersistence` instead. SQL layers
 * must not fail; wrap a fallible driver layer with `Layer.orDie`.
 *
 * Exposed from `@effect-flow/core/sqlite` so hosts that do not use SQLite never
 * load the driver.
 */
export const layerSqliteSqlClient = (filename: string): Layer.Layer<SqlClient.SqlClient> =>
  Layer.orDie(SqliteClient.layer({ filename }));
