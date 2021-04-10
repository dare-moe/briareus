---
title: Quickstart YARN
---

## Installation

Installation of Briareus libraries should be simple as with any other Java library.

Groovy users
```groovy
implementation "moe.dare.briareus:briareus-yarn:$briareusVersion"
```

Maven
```xml
<dependency>
    <groupId>moe.dare.briareus</groupId>
    <artifactId>briareus-yarn</artifactId>
    <version>${briareusVersion}</version>
</dependency>
```

:::caution

Please note that we believe that despite Log4j 1.x and commons-logging was great 
it is better to use Slf4j and newer logging systems. Log4j 1.x, commons-logging and related artifacts
are excluded from transitive dependencies. commons-logging is replaced with jcl-over-slf4j. 

:::

## Contexts

There are two context implementations:

* **Shodan** - for submitting applications into yarn and starting sensei container.
* **Sensei** - for starting containers as submitted yarn application.

Each have corresponding fluent builder:
* `moe.dare.briareus.yarn.shodan.BriareusYarnShodanContextBuilder`
* `moe.dare.briareus.yarn.sensei.BriareusYarnSenseiContextBuilder`

:::info

Briareus is Kerberos aware :dog: Just provide appropriate user.

:::

