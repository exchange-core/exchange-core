# Contributing

Use JDK 25 and Maven 3.6.3 or newer. Before opening a pull request against `master`, run the same checks as CI:

```sh
mvn verify
mvn -Pit test
```

The first command builds the library and runs unit tests and Cucumber scenarios. The second runs the integration suite,
including the trading example and journal recovery. See [Testing](README.md#testing) for stress tests and benchmarks.

In the PR, describe the behavior change and the checks you ran. For bugs, include a small reproducer or a regression test.
For performance changes, include before-and-after results and the command, JDK, and hardware used to measure them.
