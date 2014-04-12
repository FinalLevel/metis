CREATE TABLE `storage` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `hostname` varchar(255) NOT NULL DEFAULT '',
  `ip` varchar(16) NOT NULL DEFAULT '',
  `port` smallint(5) unsigned NOT NULL,
  `status` int(10) unsigned NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=MyISAM AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;

CREATE TABLE `manager` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `ip` varchar(16) NOT NULL DEFAULT '',
  `port` smallint(11) NOT NULL,
  `status` int(11) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;

CREATE TABLE `index` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `level` varchar(64) NOT NULL DEFAULT '',
  `subLevel` int(11) unsigned NOT NULL,
  `status` int(11) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `level_subLevel` (`level`,`subLevel`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;

CREATE TABLE `index_range` (
  `indexID` int(11) unsigned NOT NULL,
  `fromID` int(11) unsigned NOT NULL,
  `toID` int(11) unsigned NOT NULL,
  `managerID` int(11) unsigned NOT NULL,
  `slaveManagerID` int(11) unsigned NOT NULL,
  PRIMARY KEY (`indexID`,`fromID`,`toID`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;

