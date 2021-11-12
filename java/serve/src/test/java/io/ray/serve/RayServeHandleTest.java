package io.ray.serve;

import io.ray.api.ActorHandle;
import io.ray.api.ObjectRef;
import io.ray.api.Ray;
import io.ray.serve.api.Serve;
import io.ray.serve.generated.ActorSet;
import io.ray.serve.generated.DeploymentLanguage;
import org.testng.Assert;
import org.testng.annotations.Test;

public class RayServeHandleTest {

  @Test
  public void test() {
    boolean inited = Ray.isInitialized();
    Ray.init();

    try {
      String deploymentName = "RayServeHandleTest";
      String controllerName = deploymentName + "_controller";
      String replicaTag = deploymentName + "_replica";
      String actorName = replicaTag;
      String version = "v1";

      // Controller
      ActorHandle<DummyServeController> controllerHandle =
          Ray.actor(DummyServeController::new).setName(controllerName).remote();

      // Replica
      DeploymentConfig deploymentConfig =
          new DeploymentConfig().setDeploymentLanguage(DeploymentLanguage.JAVA.getNumber());

      Object[] initArgs = new Object[] {deploymentName, replicaTag, controllerName, new Object()};

      DeploymentInfo deploymentInfo =
          new DeploymentInfo()
              .setName(deploymentName)
              .setDeploymentConfig(deploymentConfig)
              .setDeploymentVersion(new DeploymentVersion(version))
              .setDeploymentDef("io.ray.serve.ReplicaContext")
              .setInitArgs(initArgs);

      ActorHandle<RayServeWrappedReplica> replicaHandle =
          Ray.actor(
                  RayServeWrappedReplica::new,
                  deploymentInfo,
                  replicaTag,
                  controllerName,
                  (RayServeConfig) null)
              .setName(actorName)
              .remote();
      Assert.assertTrue(replicaHandle.task(RayServeWrappedReplica::checkHealth).remote().get());

      // RayServeHandle
      RayServeHandle rayServeHandle =
          new RayServeHandle(controllerHandle, deploymentName, null, null)
              .setMethodName("getDeploymentName");
      ActorSet.Builder builder = ActorSet.newBuilder();
      builder.addNames(actorName);
      rayServeHandle.getRouter().getReplicaSet().updateWorkerReplicas(builder.build());

      // remote
      ObjectRef<Object> resultRef = rayServeHandle.remote(null);
      Assert.assertEquals((String) resultRef.get(), deploymentName);
    } finally {
      if (!inited) {
        Ray.shutdown();
      }
      Serve.setInternalReplicaContext(null);
    }
  }
}
