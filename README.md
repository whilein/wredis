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

  <a href="https://search.maven.org/artifact/io.github.whilein/wredis-api">
    <img src="https://img.shields.io/maven-central/v/io.github.whilein/wredis-api">
  </a>
</div>

## Использование

```java
public class Main {
    public static void main(final String[] args) {
        Redis redis = NioRedis.create(RedisConfig.builder(new InetSocketAddress(host, port))
                        .auth(username, password)
                        .connectTimeout(1, TimeUnit.SECONDS)
                        .build())
                .connect();

        RedisResponse response = redis.command("PING").flushAndRead();
        boolean pong = "PONG".equals(response.nextString());
    }
}
```

## Добавить в свой проект

<div>
  <a href="https://search.maven.org/artifact/io.github.whilein/wredis-api">
    <img src="https://img.shields.io/maven-central/v/io.github.whilein/wredis-api">
  </a>
</div>

### Maven

```xml

<dependencies>
    <dependency>
        <groupId>io.github.whilein</groupId>
        <artifactId>wredis-api</artifactId>
        <version>0.1.7</version>
    </dependency>

    <dependency>
        <groupId>io.github.whilein</groupId>
        <artifactId>wredis-nio-impl</artifactId>
        <version>0.1.7</version>
    </dependency>
</dependencies>
```

### Gradle

```groovy
dependencies {
    implementation 'io.github.whilein:wredis-api:0.1.7'
    implementation 'io.github.whilein:wredis-nio-impl:0.1.7'
}
```
