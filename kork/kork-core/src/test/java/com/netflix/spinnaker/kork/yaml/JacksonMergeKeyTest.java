package com.netflix.spinnaker.kork.yaml;

import org.junit.jupiter.api.Test;

/** Test to verify Jackson handles YAML merge keys */
public class JacksonMergeKeyTest {

  @Test
  public void testMergeKey() {
    try {
      String yaml =
          "---\n"
              + "- &anchor\n"
              + "  name: test\n"
              + "  type: deb\n"
              + "  value: original\n"
              + "- <<: *anchor\n"
              + "  name: override\n";

      System.out.println("=== Testing YAML merge key ===");
      System.out.println("YAML:");
      System.out.println(yaml);

      Object result = YamlHelper.newYamlSafeConstructor().load(yaml);
      System.out.println("\nResult: " + result);
      System.out.println("Result class: " + result.getClass().getName());

      if (result instanceof java.util.List) {
        java.util.List list = (java.util.List) result;
        System.out.println("List size: " + list.size());
        for (int i = 0; i < list.size(); i++) {
          System.out.println("  Item " + i + ": " + list.get(i));
          if (list.get(i) instanceof java.util.Map) {
            java.util.Map map = (java.util.Map) list.get(i);
            System.out.println("    Keys: " + map.keySet());
            System.out.println("    name: " + map.get("name"));
            System.out.println("    type: " + map.get("type"));
            System.out.println("    value: " + map.get("value"));
          }
        }
      }

      // Test simple alias without merge key
      System.out.println("\n\n=== Testing simple alias ===");
      String yaml2 =
          "---\n" + "first: &anchor\n" + "  name: test\n" + "second: *anchor\n";
      System.out.println("YAML:");
      System.out.println(yaml2);
      Object result2 = YamlHelper.newYamlSafeConstructor().load(yaml2);
      System.out.println("\nResult: " + result2);
    } catch (Exception e) {
      System.out.println("ERROR: " + e.getMessage());
      e.printStackTrace();
    }
  }
}
