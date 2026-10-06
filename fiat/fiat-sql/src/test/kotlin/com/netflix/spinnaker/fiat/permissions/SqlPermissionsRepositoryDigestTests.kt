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
import com.netflix.spinnaker.fiat.permissions.sql.tables.references.USER
import com.netflix.spinnaker.kork.dynamicconfig.DynamicConfigService
import com.netflix.spinnaker.kork.sql.config.SqlRetryProperties
import java.time.Clock
import java.util.concurrent.ConcurrentLinkedQueue
import kotlin.contracts.ExperimentalContracts
import org.jooq.DSLContext
import org.jooq.ExecuteContext
import org.jooq.SQLDialect
import org.jooq.impl.DefaultExecuteListener
import org.jooq.impl.DefaultExecuteListenerProvider
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

/**
 * A user whose stored digest matches the digest of the incoming permissions is skipped entirely. The
 * digest is only recorded after every write for that user succeeded, so anything that goes wrong is
 * repaired by the next sync rather than remembered as done.
 */
@ExperimentalContracts
internal class SqlPermissionsRepositoryDigestTests {

  private val userCount = 7
  private val batchSize = 3

  private class Statements : DefaultExecuteListener() {
    private val sql = ConcurrentLinkedQueue<String>()

    @Volatile var failPermissionDeletes = false

    override fun executeStart(ctx: ExecuteContext) {
      val normalized = ctx.sql()?.lowercase()?.replace(Regex("[`\"]"), "")?.replace(Regex("\\s+"), " ") ?: return
      if (failPermissionDeletes && normalized.startsWith("delete from fiat_permission")) {
        throw IllegalStateException("simulated failure removing stale permissions")
      }
      sql.add(normalized)
    }

    fun permissionReads() = sql.count { it.startsWith("select") && it.contains("from fiat_permission") }

    fun userUpserts() = sql.count { it.startsWith("insert into fiat_user") }

    fun clear() = sql.clear()
  }

  private class Repo(val repository: SqlPermissionsRepository, val statements: Statements, val jooq: DSLContext)

  private class Tuning {
    @Volatile var skipUnchanged = true
    @Volatile var generation = "1"
  }

  private companion object {
    // Own database names so these containers stay apart from the other test classes in one JVM.
    val dialects = listOf(SQLDialect.POSTGRES to "jdbc:tc:postgresql:12-alpine:///digestdb", SQLDialect.MYSQL to "jdbc:tc:mysql:8.0.40:///digestdb")
    val databases = mutableMapOf<SQLDialect, DSLContext>()
  }

  private val tuning = Tuning()
  private var dialectInUse: SQLDialect? = null

  @AfterEach
  fun cleanup() {
    dialectInUse?.let { databases[it]?.flushAll() }
    dialectInUse = null
  }

  private fun repo(dialect: SQLDialect, url: String): Repo {
    val jooq = databases.getOrPut(dialect) { initDatabase(url, dialect) }
    dialectInUse = dialect
    tuning.skipUnchanged = true
    tuning.generation = "1"

    val statements = Statements()
    val config =
      object : DynamicConfigService.NoopDynamicConfig() {
        @Suppress("UNCHECKED_CAST")
        override fun <T : Any> getConfig(configType: Class<T>, configName: String, defaultValue: T): T =
          when (configName) {
            "permissions-repository.sql.existing-permissions-batch-size" -> batchSize as T
            "permissions-repository.sql.digest-generation" -> tuning.generation as T
            else -> defaultValue
          }

        override fun isEnabled(flagName: String, defaultValue: Boolean): Boolean =
          if (flagName == "permissions-repository.sql.skip-unchanged-users") tuning.skipUnchanged else defaultValue
      }

    return Repo(
      SqlPermissionsRepository(
        Clock.systemUTC(),
        ObjectMapper().setSerializationInclusion(JsonInclude.Include.NON_NULL),
        jooq.configuration().derive(DefaultExecuteListenerProvider(statements)).dsl(),
        SqlRetryProperties(),
        listOf(Application(), Account(), BuildService(), ServiceAccount(), Role()),
        null,
        config
      ),
      statements,
      jooq
    )
  }

  private fun forEachDialect(scenario: (Repo) -> Unit) =
    dialects.forEach { (dialect, url) ->
      try {
        scenario(repo(dialect, url))
      } finally {
        databases[dialect]?.flushAll()
      }
    }

  private fun user(u: Int, extraAccount: String? = null, admin: Boolean = false): UserPermission =
    UserPermission()
      .setId("user$u")
      .setAdmin(admin)
      .setAccounts(setOf(Account().setName("shared")) + listOfNotNull(extraAccount).map { Account().setName(it) })
      .setApplications(setOf(Application().setName("app$u")))
      .setRoles(setOf(Role("role0"), Role("role$u")))

  private fun everyone(change: (Int) -> UserPermission = { user(it) }): Map<String, UserPermission> =
    (0 until userCount).associate { "user$it" to change(it) }

  private fun storedHash(repo: Repo, id: String): String? =
    repo.jooq.select(USER.PERMISSIONS_HASH).from(USER).where(USER.ID.eq(id)).fetchOne(USER.PERMISSIONS_HASH)

  private fun accountsOf(repo: Repo, id: String) =
    repo.repository.get(id).orElseThrow { AssertionError("no permission stored for $id") }.accounts.map { it.name }.toSet()

  @Test
  fun `unchanged users are skipped entirely`() = forEachDialect { repo ->
    repo.repository.putAllById(everyone())
    (0 until userCount).forEach { assertNotNull(storedHash(repo, "user$it"), "digest recorded for user$it") }

    repo.statements.clear()
    repo.repository.putAllById(everyone())

    assertEquals(0, repo.statements.permissionReads(), "no existing-permission reads for unchanged users")
    assertEquals(0, repo.statements.userUpserts(), "no user upserts for unchanged users")
    assertEquals(setOf("shared"), accountsOf(repo, "user3"))
  }

  @Test
  fun `only the changed user is rewritten`() = forEachDialect { repo ->
    repo.repository.putAllById(everyone())
    val before = (0 until userCount).associate { "user$it" to storedHash(repo, "user$it") }

    repo.statements.clear()
    repo.repository.putAllById(everyone { if (it == 4) user(it, extraAccount = "extra") else user(it) })

    assertEquals(1, repo.statements.permissionReads(), "existing permissions are read for the one changed user")
    assertEquals(1, repo.statements.userUpserts(), "one user upsert")
    assertEquals(setOf("shared", "extra"), accountsOf(repo, "user4"))
    (0 until userCount).forEach {
      if (it == 4) {
        assertTrue(before["user4"] != storedHash(repo, "user4"), "digest updated for the changed user")
      } else {
        assertEquals(before["user$it"], storedHash(repo, "user$it"), "digest untouched for user$it")
      }
    }
  }

  @Test
  fun `removing a resource is detected and the row is deleted`() = forEachDialect { repo ->
    repo.repository.putAllById(everyone { user(it, extraAccount = "extra") })
    assertEquals(setOf("shared", "extra"), accountsOf(repo, "user2"))

    repo.repository.putAllById(everyone())

    assertEquals(setOf("shared"), accountsOf(repo, "user2"))
  }

  @Test
  fun `a change to only the admin flag is detected`() = forEachDialect { repo ->
    repo.repository.putAllById(everyone())
    assertFalse(repo.repository.get("user1").get().isAdmin)

    repo.repository.putAllById(everyone { if (it == 1) user(it, admin = true) else user(it) })

    assertTrue(repo.repository.get("user1").get().isAdmin)
  }

  @Test
  fun `a user with no stored digest is rewritten and then recorded`() = forEachDialect { repo ->
    repo.repository.putAllById(everyone())
    // as if the row predates the migration
    repo.jooq.update(USER).setNull(USER.PERMISSIONS_HASH).where(USER.ID.eq("user5")).execute()

    repo.statements.clear()
    repo.repository.putAllById(everyone())
    assertEquals(1, repo.statements.userUpserts(), "only the user without a digest is rewritten")
    assertNotNull(storedHash(repo, "user5"))

    repo.statements.clear()
    repo.repository.putAllById(everyone())
    assertEquals(0, repo.statements.userUpserts(), "now recorded, so skipped")
  }

  @Test
  fun `skipping can be turned off`() = forEachDialect { repo ->
    repo.repository.putAllById(everyone())
    tuning.skipUnchanged = false

    repo.statements.clear()
    repo.repository.putAllById(everyone())

    assertEquals(userCount, repo.statements.userUpserts(), "every user is rewritten when skipping is off")
    assertNotNull(storedHash(repo, "user0"), "digests stay accurate while skipping is off")
  }

  @Test
  fun `changing the digest generation rewrites every user`() = forEachDialect { repo ->
    repo.repository.putAllById(everyone())

    tuning.generation = "2"
    repo.statements.clear()
    repo.repository.putAllById(everyone())
    assertEquals(userCount, repo.statements.userUpserts())

    repo.statements.clear()
    repo.repository.putAllById(everyone())
    assertEquals(0, repo.statements.userUpserts(), "and they are skipped again under the new generation")
  }

  @Test
  fun `a failed removal of stale permissions is retried by the next sync`() = forEachDialect { repo ->
    repo.repository.putAllById(everyone { user(it, extraAccount = "extra") })
    val hashBefore = storedHash(repo, "user3")

    repo.statements.failPermissionDeletes = true
    repo.repository.putAllById(everyone())
    // the delete failed, so the stale row is still there...
    assertEquals(setOf("shared", "extra"), accountsOf(repo, "user3"))
    // ...and the user must not have been recorded as up to date
    assertEquals(hashBefore, storedHash(repo, "user3"), "digest not advanced after a failed write")

    repo.statements.failPermissionDeletes = false
    repo.repository.putAllById(everyone())
    assertEquals(setOf("shared"), accountsOf(repo, "user3"), "the next sync removes the stale permission")
    assertTrue(storedHash(repo, "user3") != hashBefore)
  }

  @Test
  fun `a removed user is rewritten if they come back`() = forEachDialect { repo ->
    repo.repository.putAllById(everyone())
    repo.repository.remove("user2")
    assertTrue(repo.repository.get("user2").isEmpty)
    assertNull(storedHash(repo, "user2"))

    repo.repository.putAllById(everyone())

    assertEquals(setOf("shared"), accountsOf(repo, "user2"))
  }
}
