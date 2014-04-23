CREATE TABLE `storage_group` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `name` varchar(255) NOT NULL DEFAULT '',
  PRIMARY KEY (`id`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8

CREATE TABLE `storage` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `groupID` int(255) unsigned NOT NULL,
  `ip` varchar(16) NOT NULL DEFAULT '',
  `port` smallint(5) unsigned NOT NULL,
  `status` int(10) unsigned NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8

CREATE TABLE `manager` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `cmdIp` varchar(16) NOT NULL DEFAULT '',
  `cmdPort` smallint(11) NOT NULL,
  `webDavIp` varchar(16) NOT NULL,
  `webDavPort` smallint(11) NOT NULL,
  `webIp` varchar(16) NOT NULL,
  `webPort` smallint(11) NOT NULL,
  `status` int(11) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8

CREATE TABLE `index` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `level` varchar(64) NOT NULL DEFAULT '',
  `subLevel` int(11) unsigned NOT NULL,
  `status` int(11) NOT NULL,
  `rangeSize` int(10) unsigned NOT NULL DEFAULT '131072',
  PRIMARY KEY (`id`),
  UNIQUE KEY `level_subLevel` (`level`,`subLevel`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;

CREATE TABLE `index_range` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `indexID` int(11) unsigned NOT NULL,
  `rangeIndex` int(11) unsigned NOT NULL,
  `managerID` int(11) unsigned NOT NULL,
  `storageIDs` varchar(255) NOT NULL DEFAULT '',
  PRIMARY KEY (`id`),
  UNIQUE KEY `index_range` (`indexID`,`rangeIndex`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8
