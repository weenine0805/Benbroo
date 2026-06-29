-- Benbroo Database Schema
CREATE DATABASE IF NOT EXISTS benbroo DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
USE benbroo;

-- Namespaces
CREATE TABLE IF NOT EXISTS `namespace` (
    `id`          VARCHAR(128) NOT NULL,
    `name`        VARCHAR(256) NOT NULL,
    `description` VARCHAR(1024) DEFAULT '',
    `created_at`  DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    `updated_at`  DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
    PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Services
CREATE TABLE IF NOT EXISTS `service` (
    `id`                BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    `namespace_id`      VARCHAR(128) NOT NULL DEFAULT 'public',
    `group_name`        VARCHAR(128) NOT NULL DEFAULT 'DEFAULT_GROUP',
    `service_name`      VARCHAR(512) NOT NULL,
    `protect_threshold` DOUBLE NOT NULL DEFAULT 0.0,
    `metadata`          TEXT,
    `created_at`        DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    `updated_at`        DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_ns_group_service` (`namespace_id`, `group_name`, `service_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Service Instances
CREATE TABLE IF NOT EXISTS `instance` (
    `id`           BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    `namespace_id` VARCHAR(128) NOT NULL DEFAULT 'public',
    `group_name`   VARCHAR(128) NOT NULL DEFAULT 'DEFAULT_GROUP',
    `service_name` VARCHAR(512) NOT NULL,
    `cluster_name` VARCHAR(128) NOT NULL DEFAULT 'DEFAULT',
    `ip`           VARCHAR(64) NOT NULL,
    `port`         INT NOT NULL,
    `weight`       DOUBLE NOT NULL DEFAULT 1.0,
    `healthy`      TINYINT(1) NOT NULL DEFAULT 1,
    `enabled`      TINYINT(1) NOT NULL DEFAULT 1,
    `ephemeral`    TINYINT(1) NOT NULL DEFAULT 1,
    `metadata`     TEXT,
    `last_beat`    DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    `created_at`   DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    `updated_at`   DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_ns_svc_ip_port` (`namespace_id`, `group_name`, `service_name`, `cluster_name`, `ip`, `port`),
    KEY `idx_ns_service` (`namespace_id`, `group_name`, `service_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Config Items
CREATE TABLE IF NOT EXISTS `config_info` (
    `id`           BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    `namespace_id` VARCHAR(128) NOT NULL DEFAULT 'public',
    `group_name`   VARCHAR(128) NOT NULL DEFAULT 'DEFAULT_GROUP',
    `data_id`      VARCHAR(512) NOT NULL,
    `content`      LONGTEXT NOT NULL,
    `md5`          VARCHAR(64) NOT NULL,
    `type`         VARCHAR(64) DEFAULT 'text',
    `created_at`   DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    `updated_at`   DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_ns_group_dataid` (`namespace_id`, `group_name`, `data_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Config History
CREATE TABLE IF NOT EXISTS `config_history` (
    `id`           BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    `namespace_id` VARCHAR(128) NOT NULL DEFAULT 'public',
    `group_name`   VARCHAR(128) NOT NULL DEFAULT 'DEFAULT_GROUP',
    `data_id`      VARCHAR(512) NOT NULL,
    `content`      LONGTEXT NOT NULL,
    `md5`          VARCHAR(64) NOT NULL,
    `op_type`      VARCHAR(32) NOT NULL DEFAULT 'U',
    `created_at`   DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    PRIMARY KEY (`id`),
    KEY `idx_ns_group_dataid` (`namespace_id`, `group_name`, `data_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Cluster Nodes
CREATE TABLE IF NOT EXISTS `cluster_node` (
    `id`         BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    `address`    VARCHAR(256) NOT NULL,
    `state`      VARCHAR(32) NOT NULL DEFAULT 'UP',
    `last_beat`  DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    `created_at` DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    `updated_at` DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_address` (`address`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Insert default namespace
INSERT INTO `namespace` (`id`, `name`, `description`) VALUES ('public', 'Public', 'Default public namespace')
ON DUPLICATE KEY UPDATE `name` = `name`;
