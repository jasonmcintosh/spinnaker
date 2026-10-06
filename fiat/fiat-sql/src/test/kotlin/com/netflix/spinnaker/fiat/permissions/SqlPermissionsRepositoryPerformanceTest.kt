/*
 * Copyright 2026 Spinnaker Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.spinnaker.fiat.permissions

import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.databind.ObjectMapper
import com.netflix.spinnaker.fiat.model.UserPermission
import com.netflix.spinnaker.fiat.model.resources.Account
import com.netflix.spinnaker.fiat.model.resources.Application
import com.netflix.spinnaker.fiat.model.resources.BuildService
import com.netflix.spinnaker.fiat.model.resources.Role
import com.netflix.spinnaker.fiat.model.resources.ServiceAccount
import com.netflix.spinnaker.kork.dynamicconfig.DynamicConfigService
import com.netflix.spinnaker.kork.sql.config.SqlRetryProperties
import java.time.Clock
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.Executors
import kotlin.contracts.ExperimentalContracts
import kotlinx.coroutines.asCoroutineDispatcher
import com.netflix.spinnaker.kork.jedis.JedisClientDelegate
import io.github.resilience4j.retry.RetryRegistry
import org.jooq.DSLContext
import org.jooq.ExecuteContext
import org.jooq.SQLDialect
import org.jooq.impl.DefaultExecuteListener
import org.jooq.impl.DefaultExecuteListenerProvider
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.condition.EnabledIfSystemProperty
import org.slf4j.LoggerFactory
import org.testcontainers.containers.GenericContainer
import org.testcontainers.utility.DockerImageName
import redis.clients.jedis.JedisPool

/**
 * Baseline measurements for the SQL hot paths described in fiat-performance-fix.md.
 *
 * These tests count the statements [SqlPermissionsRepository] issues, rather than asserting on wall
 * clock time, so they are deterministic. They currently assert the *problematic* behaviour so that
 * the cost is visible and reproducible. When the repository is optimized:
 * 1. Run this class before the change and keep the logged "PERF" numbers as the "before".
 * 2. Update (or flip) the assertions to the new, cheaper expectations and record the "after".
 * 3. Disable (or delete) the class if the numbers are no longer interesting.
 *
 * Opt in with -Dfiat.perf.enabled=true (it takes minutes and several GB of heap at the default size).
 * Look for lines starting with "PERF" in the test output.
 */
@Tag("performance")
@EnabledIfSystemProperty(named = "fiat.perf.enabled", matches = "true")
@ExperimentalContracts
internal class SqlPermissionsRepositoryPerformanceTest {

  private val log = LoggerFactory.getLogger(SqlPermissionsRepositoryPerformanceTest::class.java)

  // Large-scale defaults; override with e.g. -Dfiat.perf.users=500 for a quick local run.
  private val userCount = intProp("users", 2000)
  private val applicationCount = intProp("applications", 15000)
  private val applicationsPerUser = intProp("applicationsPerUser", 200)
  private val accountCount = intProp("accounts", 100)
  private val accountsPerUser = intProp("accountsPerUser", 20)
  private val roleCount = intProp("roles", 25000)
  private val rolesPerUser = intProp("rolesPerUser", 5000)

  // Write tuning knobs, so before/after runs can compare configurations of the same code.
  // concurrency > 0 enables the repository's async path (the Hikari pool in TestDatabase.kt caps this at 5).
  private val writeBatchSize = intProp("writeBatch", 100)
  private val concurrency = intProp("concurrency", 0)

  /** Every statement executed through the instrumented DSLContext. */
  private class Statements : DefaultExecuteListener() {
    val sql = ConcurrentLinkedQueue<String>()

    // Drops quoting and schema qualifiers so "`db`.`RESOURCE`" and "public.resource" both read "resource".
    private fun normalize(raw: String) =
      raw.lowercase().replace(Regex("[`\"]"), "").replace(Regex("\\s+"), " ").replace(Regex("\\b\\w+\\.(?=\\w)"), "")

    /** Total nanoseconds spent in each distinct statement shape (first 100 chars), for finding hot spots. */
    val nanosByShape = java.util.concurrent.ConcurrentHashMap<String, java.util.concurrent.atomic.LongAdder>()
    private val started = ThreadLocal<Long>()

    override fun executeStart(ctx: ExecuteContext) {
      ctx.sql()?.let { sql.add(normalize(it)) }
      started.set(System.nanoTime())
    }

    override fun executeEnd(ctx: ExecuteContext) {
      val shape = ctx.sql()?.let { normalize(it).take(100) } ?: return
      nanosByShape.computeIfAbsent(shape) { java.util.concurrent.atomic.LongAdder() }.add(System.nanoTime() - started.get())
    }

    fun clear() {
      sql.clear()
      nanosByShape.clear()
    }

    /** SELECTs against RESOURCE with no WHERE clause, i.e. a full table read. */
    fun fullResourceScans() =
      sql.count { it.startsWith("select") && it.contains("from fiat_resource") && !it.contains("where") }

    /** SELECTs that read RESOURCE bodies (what getUserRoles does for every ROLE). */
    fun resourceBodyReads() =
      sql.count { it.startsWith("select") && it.contains("from fiat_resource") && it.contains("body") }

    /** Per-user SELECTs from PERMISSION (the N+1 in putUserPermissions). */
    fun permissionSelects() =
      sql.count { it.startsWith("select") && it.contains("from fiat_permission") }

    fun selects() = sql.count { it.startsWith("select") }

    fun writes() = sql.count { !it.startsWith("select") }

    fun total() = sql.size
  }

  private fun tuning(): DynamicConfigService =
    object : DynamicConfigService.NoopDynamicConfig() {
      private val overrides =
        mutableMapOf<String, Any>("permissions-repository.sql.write-batch-size" to writeBatchSize).apply {
          if (concurrency > 0) {
            put("permissions-repository.sql.max-query-concurrency", concurrency)
            // useAsync() needs more than 2x this many users before it goes async.
            put("permissions-repository.sql.read-batch-size", 10)
          } else {
            put("permissions-repository.sql.max-query-concurrency", 1)
          }
        }

      @Suppress("UNCHECKED_CAST")
      override fun <T : Any> getConfig(configType: Class<T>, configName: String, defaultValue: T): T =
        overrides[configName] as T? ?: defaultValue
    }

  private inner class Fixture(val jooq: DSLContext, val statements: Statements) {
    val repository: SqlPermissionsRepository
    init {
      val instrumented =
        jooq
          .configuration()
          .derive(DefaultExecuteListenerProvider(statements))
          .dsl()
      repository =
        SqlPermissionsRepository(
          Clock.systemUTC(),
          ObjectMapper().setSerializationInclusion(JsonInclude.Include.NON_NULL),
          instrumented,
          SqlRetryProperties(),
          listOf(Application(), Account(), BuildService(), ServiceAccount(), Role()),
          if (concurrency > 0) Executors.newFixedThreadPool(concurrency).asCoroutineDispatcher() else null,
          tuning()
        )
    }
  }

  // Numbers are only logged unless -Dfiat.perf.enforce=true, which also asserts the expected statement counts
  // (batched permission reads). Leave it off to measure code that predates a fix.
  private fun assertEquals(expected: Int, actual: Int, message: String) {
    if (enforce) org.junit.jupiter.api.Assertions.assertEquals(expected, actual, message)
  }

  private fun assertTrue(condition: Boolean, message: String) {
    if (enforce) org.junit.jupiter.api.Assertions.assertTrue(condition, message)
  }

  private companion object {
    val enforce = System.getProperty("fiat.perf.enforce") == "true"

    fun intProp(name: String, default: Int) = System.getProperty("fiat.perf.$name")?.toInt() ?: default

    // One database per dialect for the whole class: initDatabase() can't be run twice against the
    // same Testcontainers JDBC url.
    val databases = mutableMapOf<SQLDialect, DSLContext>()
  }

  private var current: DSLContext? = null

  @AfterEach
  fun cleanup() {
    current?.flushAll()
    current = null
  }

  private fun fixture(jdbcUrl: String, dialect: SQLDialect): Fixture {
    val jooq = databases.getOrPut(dialect) { initDatabase(jdbcUrl, dialect) }
    current = jooq
    return Fixture(jooq, Statements())
  }

  /**
   * Users share a large pool of applications/accounts (as real Fiat deployments do) and each has a
   * handful of roles drawn from a smaller shared pool.
   */
  // Resource objects are immutable here and shared across users to keep the test heap manageable
  // (2500 users x 5000 roles is 12.5M set entries before any strings are allocated).
  private val accountPool by lazy { (0 until accountCount).map { Account().setName("account$it") } }
  private val applicationPool by lazy { (0 until applicationCount).map { Application().setName("app$it") } }
  private val rolePool by lazy { (0 until roleCount).map { Role("role$it") } }

  private fun users(): Map<String, UserPermission> =
    (0 until userCount).associate { u ->
      val id = "user$u"
      id to
        UserPermission()
          .setId(id)
          .setAccounts((0 until accountsPerUser).map { accountPool[(u * 7 + it) % accountCount] }.toSet())
          .setApplications(
            (0 until applicationsPerUser).map { applicationPool[(u * 31 + it * 71) % applicationCount] }.toSet()
          )
          .setRoles((0 until rolesPerUser).map { rolePool[(u * 13 + it * 97) % roleCount] }.toSet())
    }

  private fun report(label: String, s: Statements, elapsedMs: Long) {
    log.info(
      "PERF [writeBatch={} concurrency={}] {} -> total={} selects={} writes={} fullResourceScans={} permissionSelects={} elapsedMs={}",
      writeBatchSize, concurrency, label, s.total(), s.selects(), s.writes(), s.fullResourceScans(), s.permissionSelects(), elapsedMs
    )
    s.nanosByShape.entries.sortedByDescending { it.value.sum() }.take(4).forEach {
      log.info("PERF   {}ms in: {}", it.value.sum() / 1_000_000, it.key)
    }
  }

  private inline fun timed(block: () -> Unit): Long {
    val start = System.nanoTime()
    block()
    return (System.nanoTime() - start) / 1_000_000
  }

  private fun syncBaseline(label: String, fixture: Fixture) {
    val permissions = users()
    val s = fixture.statements

    // Initial sync: everything is new.
    s.clear()
    val initialMs = timed { fixture.repository.putAllById(permissions) }
    report("$label initial putAllById($userCount users)", s, initialMs)
    val maxUserReads = (userCount + 99) / 100
        assertTrue(
      s.permissionSelects() <= maxUserReads,
      "PERMISSION is read once per batch of users, not per user (issue #2), was ${s.permissionSelects()}"
    )

    // A production table would have been analyzed by autovacuum by now; a freshly bulk-loaded test table has
    // no statistics, which would make the planner choose seq scans for the hash lookups.
    if (fixture.jooq.dialect() == SQLDialect.POSTGRES) {
      fixture.jooq.execute("analyze")
    }

    // Resync with identical data: the ideal cost is a handful of reads and one user upsert each.
    s.clear()
    val resyncMs = timed { fixture.repository.putAllById(permissions) }
    report("$label unchanged resync putAllById($userCount users)", s, resyncMs)
    if (fixture.jooq.dialect() == SQLDialect.POSTGRES) {
      val bytes =
        fixture.jooq.fetchValue(
          "select pg_total_relation_size('fiat_permission') + pg_total_relation_size('fiat_resource') + " +
            "pg_total_relation_size('fiat_user')"
        ) as Number
      log.info("PERF [sql storage] fiat_* tables and indexes: {} MB", bytes.toLong() / (1024 * 1024))
    }
    assertTrue(s.permissionSelects() <= maxUserReads, "batched PERMISSION reads on an unchanged resync")
  }

  private fun roleReads(label: String, fixture: Fixture) {
    val s = fixture.statements

    s.clear()
    val allMs = timed { fixture.repository.getAllById() }
    report("$label getAllById", s, allMs)
    assertEquals(1, s.resourceBodyReads(), "getUserRoles loads and deserializes every ROLE body (issue #3)")

    s.clear()
    val someMs = timed { fixture.repository.getAllByRoles(listOf("role0")) }
    report("$label getAllByRoles([role0])", s, someMs)
    assertEquals(
      1,
      s.resourceBodyReads(),
      "getAllByRoles still loads every ROLE body, not just the requested role (issue #3)"
    )
  }

  // Separate "perfdb" urls keep these containers apart from SqlPermissionsRepositoryTests in the same JVM.
  // Seeding is the expensive part, so each dialect runs sync then role reads against one data set.
  private fun run(label: String, fixture: Fixture) {
    syncBaseline(label, fixture)
    roleReads(label, fixture)
    singleUserSync(label, fixture)
    getCost(label, fixture)
  }

  /** The per-request path: every authorization check for a user not in Gate's cache lands here. */
  private fun getCost(label: String, fixture: Fixture) {
    val s = fixture.statements
    val samples = minOf(20, userCount)

    s.clear()
    var loaded = 0
    val ms = timed {
      repeat(samples) {
        loaded += fixture.repository.get("user${it * (userCount / samples)}").get().roles.size
      }
    }
    report("$label get() x$samples (avgMs=${ms / samples}, avgRoles=${loaded / samples})", s, ms)
    assertTrue(loaded / samples >= rolesPerUser, "get() returns every role for the user")
  }

  /** A login or role-change style sync of one user against a fully populated table. */
  private fun singleUserSync(label: String, fixture: Fixture) {
    val s = fixture.statements
    val one = users().getValue("user0")

    s.clear()
    val ms = timed { fixture.repository.put(one) }
    report("$label single-user put", s, ms)
  }

  @Test
  fun `postgres cost at scale`() =
    run("postgres", fixture("jdbc:tc:postgresql:12-alpine:///perfdb", SQLDialect.POSTGRES))

  // MySQL is roughly 8x slower than Postgres here (tens of minutes at the default size), so opt in with
  // -Dfiat.perf.mysql=true, ideally with a smaller -Dfiat.perf.users.
  @Test
  @EnabledIfSystemProperty(named = "fiat.perf.mysql", matches = "true")
  fun `mysql cost at scale`() = run("mysql", fixture("jdbc:tc:mysql:8.0.40:///perfdb", SQLDialect.MYSQL))

  /**
   * The same operations against RedisPermissionsRepository (Valkey 8, as its own tests use) with its
   * production defaults, for a like-for-like comparison with the SQL numbers. Redis has no statement
   * counter here, so only wall time is reported.
   */
  @Test
  fun `redis cost at scale`() {
    GenericContainer<Nothing>(DockerImageName.parse("valkey/valkey:8")).apply { withExposedPorts(6379) }.use { valkey ->
      valkey.start()
      JedisPool(valkey.host, valkey.getMappedPort(6379)).use { pool ->
        val props = RedisPermissionRepositoryConfigProps().apply { prefix = "perf" }
        val repository =
          RedisPermissionsRepository(
            ObjectMapper().setSerializationInclusion(JsonInclude.Include.NON_NULL),
            JedisClientDelegate(pool),
            listOf(Application(), Account(), BuildService(), ServiceAccount(), Role()),
            props,
            RetryRegistry.ofDefaults()
          )
        val permissions = users()

        fun line(what: String, ms: Long) =
          log.info("PERF [redis syncThreads={}] {} elapsedMs={}", props.repository.syncThreads, what, ms)

        line("initial putAllById($userCount users)", timed { repository.putAllById(permissions) })
        line("unchanged resync putAllById($userCount users)", timed { repository.putAllById(permissions) })
        line("getAllById", timed { repository.getAllById() })
        line("getAllByRoles([role0])", timed { repository.getAllByRoles(listOf("role0")) })
        line("single-user put", timed { repository.put(permissions.getValue("user0")) })

        pool.resource.use { jedis ->
          val used = Regex("used_memory:(\\d+)").find(jedis.info("memory"))?.groupValues?.get(1)?.toLong() ?: 0L
          log.info("PERF [redis storage] used_memory: {} MB, keys: {}", used / (1024 * 1024), jedis.dbSize())
        }

        val samples = minOf(20, userCount)
        var loaded = 0
        val ms =
          timed {
            repeat(samples) { loaded += repository.get("user${it * (userCount / samples)}").get().roles.size }
          }
        line("get() x$samples (avgMs=${ms / samples}, avgRoles=${loaded / samples})", ms)
        assertTrue(loaded / samples >= rolesPerUser, "redis get() returns every role for the user")
      }
    }
  }
}
