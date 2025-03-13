package com.iterable.pulsarexample.util

import org.apache.pulsar.client.admin.PulsarAdmin
import org.apache.pulsar.client.api.Schema
import org.apache.pulsar.common.policies.data.TenantInfo
import org.apache.pulsar.common.schema.SchemaInfo
import play.api.libs.json.Format
import play.api.libs.json.Json
import PulsarClientWrapper.ClientConfig

import scala.jdk.CollectionConverters.*
import scala.util.Try

case class TestPayload(name: String, numPublishes: Int)
object TestPayload {
  def apply(name: String): TestPayload = TestPayload(name, 0)
  def apply(message: TestPayload): TestPayload = TestPayload(message.name, message.numPublishes + 1)

  implicit val format: Format[TestPayload] = Json.format[TestPayload]

  val schema: Schema[TestPayload] = new Schema[TestPayload] {
    override def encode(message: TestPayload): Array[Byte] = format.writes(message).toString().getBytes

    override def getSchemaInfo: SchemaInfo = null

    override def decode(bytes: Array[Byte], schemaVersion: Array[Byte]): TestPayload = Json.parse(bytes).as[TestPayload]

    override def clone(): Schema[TestPayload] = this
  }

}

/**
 * Contains the namespace information for the test environment
 * @param tenant
 *   The tenant name created for testing
 * @param namespace
 *   The namespace name created for testing
 */
case class TestNamespace(
  tenant: String,
  namespace: String
) {
  def getFullNamespace: String = s"$tenant/$namespace"
}

/**
 * Provides utilities for setting up a Pulsar test environment
 */
object PulsarClientSetup {

  /**
   * Sets up a Pulsar test environment with a new tenant and namespace
   *
   * @param adminUrl
   *   The URL for the Pulsar admin API (default: http://localhost:8080)
   * @param serviceUrl
   *   The URL for the Pulsar broker (default: pulsar://localhost:6650)
   * @return
   *   A tuple containing (PulsarClient, PulsarAdmin, TestNamespace) wrapped in Try
   */
  def setup(
    tenant: String,
    namespace: String,
    adminUrl: String = "http://localhost:8080",
    serviceUrl: String = "pulsar://localhost:6650"
  ) = {
    for {
      // Create the Pulsar client
      client <- PulsarClientWrapper.createClient(
        ClientConfig(
          serviceUrl = serviceUrl
        )
      )

      // Create the admin client
      admin <- Try {
        PulsarAdmin
          .builder()
          .serviceHttpUrl(adminUrl)
          .build()
      }

      // Create tenant and namespace
      namespace <- setupNamespace(admin, tenant, namespace)

    } yield (client, admin, namespace)
  }

  private def setupNamespace(admin: PulsarAdmin, tenant: String, namespace: String): Try[TestNamespace] = {
    Try {

      // Create tenant if it doesn't exist
      if (!admin.tenants().getTenants.asScala.toList.contains(tenant)) {
        admin
          .tenants()
          .createTenant(
            tenant,
            TenantInfo
              .builder()
              .allowedClusters(Set("standalone").asJava)
              .build()
          )
      }

      // Create namespace if it doesn't exist
      val fullNamespace = s"$tenant/$namespace"
      if (!admin.namespaces().getNamespaces(tenant).asScala.toList.contains(fullNamespace)) {
        admin.namespaces().createNamespace(fullNamespace)
      }

      TestNamespace(tenant, namespace)
    }
  }

  /**
   * Cleans up the test environment
   */
  def cleanup(admin: PulsarAdmin, namespace: TestNamespace): Try[Unit] = {
    Try {
      // Delete namespace
      admin.namespaces().deleteNamespace(namespace.getFullNamespace, true)

      // Delete tenant
      admin.tenants().deleteTenant(namespace.tenant, true)

      // Close admin client
      admin.close()
    }
  }
}
