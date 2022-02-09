<!-- @formatter:off  -->

# wredis

<div align="center">
  <a href="https://github.com/whilein/wredis/blob/master/LICENSE">
    <img src="https://img.shields.io/github/license/whilein/wredis">
  </a>

  <a href="https://discord.gg/ANEHruraCc">
    <img src="https://img.shields.io/discord/819859288049844224?logo=discord">
  </a>

  <a href="https://github.com/whilein/wredis/issues">
    <img src="https://img.shields.io/github/issues/whilein/wredis">
  </a>

  <a href="https://github.com/whilein/wredis/pulls">
    <img src="https://img.shields.io/github/issues-pr/whilein/wredis">
  </a>

  <a href="https://search.maven.org/artifact/io.github.whilein/wredis">
    <img src="https://img.shields.io/maven-central/v/io.github.whilein/wredis">
  </a>
</div>

## Использование

```java
public class Main {
    public static void main(final String[] args) {
        Redis redis = NioRedis.create(new InetSocketAddress(host, port));
        redis.auth(username, password);

        redis.command("PING");
        redis.flush();

        RedisResponse response = redis.read();
        boolean pong = "PONG".equals(response.nextString());
    }
}
```

## Добавить в свой проект

<div>
  <a href="https://search.maven.org/artifact/io.github.whilein/wredis">
    <img src="https://img.shields.io/maven-central/v/io.github.whilein/wredis">
  </a>
</div>

### Maven

```xml

<dependencies>
    <dependency>
        <groupId>io.github.whilein</groupId>
        <artifactId>wredis</artifactId>
        <version>0.1.0</version>
    </dependency>
</dependencies>
```

### Gradle

```groovy
dependencies {
    implementation 'io.github.whilein:wredis:0.1.0'
}
```
