/*
 Navicat Premium Data Transfer

 Source Server         : sohuglobal
 Source Server Type    : MySQL
 Source Server Version : 80028
 Source Host           : sohuglobal.mysql.rds.aliyuncs.com:3306
 Source Schema         : sohu-cloud

 Target Server Type    : MySQL
 Target Server Version : 80028
 File Encoding         : 65001

 Date: 12/11/2024 15:20:44
*/

SET NAMES utf8mb4;
SET
FOREIGN_KEY_CHECKS = 0;

-- ----------------------------
-- Table structure for sohu_product_category_pc_copy1
-- ----------------------------
DROP TABLE IF EXISTS `sohu_product_category_pc_copy1`;
CREATE TABLE `sohu_product_category_pc_copy1`
(
    `id`          bigint   NOT NULL AUTO_INCREMENT,
    `pid`         bigint unsigned NOT NULL DEFAULT '0' COMMENT '父级ID',
    `name`        varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL COMMENT '名称',
    `icon`        varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL COMMENT 'icon',
    `level`       int      NOT NULL                                             DEFAULT '1' COMMENT '级别:1，2，3',
    `sort`        int      NOT NULL                                             DEFAULT '999' COMMENT '排序',
    `is_show`     tinyint(1) NOT NULL DEFAULT '1' COMMENT '显示状态',
    `is_del`      tinyint unsigned DEFAULT '0' COMMENT '是否删除',
    `create_time` datetime NOT NULL                                             DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `update_time` datetime NOT NULL                                             DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    `sys_source`  varchar(32) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci  DEFAULT 'sohuglobal' COMMENT '系统来源(sohuglobal:狐少少,minglereels:海外短剧),用来区分是哪个系统',
    PRIMARY KEY (`id`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=417 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci ROW_FORMAT=DYNAMIC COMMENT='商品分类表（平台）';

-- ----------------------------
-- Records of sohu_product_category_pc_copy1
-- ----------------------------
BEGIN;
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (1, 0, '饮品', 'https://sohugloba.oss-cn-beijing.aliyuncs.com/2023/09/20/e948541315a74d1f96e0f57ee79e867b.png',
        1, 1, 1, 1, '2023-09-20 21:49:28', '2024-10-22 15:52:51', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (4, 0, '水果蔬菜',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2023/12/04/867af2475f58449f87733768b2c31c04.jpeg', 1, 1, 1, 1,
        '2023-09-22 14:37:06', '2024-10-22 16:00:06', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (5, 4, '坚果', 'https://sohugloba.oss-cn-beijing.aliyuncs.com/2023/12/04/9ef80f497d0046c09f4fbb39e20ac503.jpg',
        2, 3, 0, 1, '2023-09-22 14:37:18', '2024-10-22 16:00:24', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (7, 4, '水果', 'https://sohugloba.oss-cn-beijing.aliyuncs.com/2023/12/04/53dec1043d454596b06037ef2feb1291.jpg',
        2, 2, 1, 1, '2023-09-22 17:21:38', '2024-10-22 16:00:39', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (8, 7, '热带水果',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2023/12/04/e209501d00764183a31b15829cb3192c.jpeg', 3, 5, 1, 1,
        '2023-09-22 17:22:13', '2024-10-22 16:01:32', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (9, 5, '开心果',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2023/12/04/654d4b9d6a92459e970a76534c9e3319.jpeg', 3, 3, 0, 1,
        '2023-09-22 17:22:37', '2024-10-22 16:01:39', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (11, 0, '服装',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/04/26/a570948e11f74cad9fece358d1c93442_2000x2000.jpg', 1, 1,
        1, 1, '2024-01-04 11:40:03', '2024-10-22 16:02:13', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (12, 0, '五金',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/04/10/4e47b0bac291481191757ade7faca1be_173x139.png', 1, 1,
        1, 1, '2024-04-10 10:36:26', '2024-10-22 16:02:40', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (13, 12, '元器件',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/04/10/148b0c14321b44058bd01421b49a3b49_108x108.png', 2, 1,
        1, 1, '2024-04-10 10:37:29', '2024-10-22 16:02:46', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (14, 13, '手动工具',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/04/10/207c4e655c6b4e74967dfa103a251fc7_220x228.png', 3, 1,
        1, 1, '2024-04-10 10:38:20', '2024-10-22 16:02:55', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (15, 13, '测量工具',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/04/10/b36a319079e94514be3b9ab8db35cbe9_254x230.png', 3, 2,
        1, 1, '2024-04-10 10:38:39', '2024-10-22 16:02:59', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (16, 11, '上衣',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/08/19/fd4c3f74fb5a4d24b0bc38e263f6e58d_800x800.jpg', 2, 1,
        1, 1, '2024-04-25 19:52:47', '2024-10-22 16:03:13', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (17, 0, '陶瓷',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/04/26/3c9bdac33e764f4a8594fb5715a5a788_800x800.jpg', 1, 1,
        1, 1, '2024-04-26 15:03:41', '2024-10-22 16:03:28', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (18, 17, '手工',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/04/26/7ffed5ad2ad94b86a0656e82e434895d_800x800.jpg', 2, 1,
        1, 1, '2024-04-26 15:04:03', '2024-10-22 16:03:33', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (19, 16, '短袖',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/08/19/d0e198a15703428d9b98c9280f50b5c0_800x799.jpg', 3, 1,
        1, 1, '2024-08-19 14:40:21', '2024-10-22 16:03:41', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (20, 0, '门票',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/09/09/b53c3269f42e44a09f4ec5c317b7802c_1765x580.png', 1, 2,
        1, 1, '2024-09-09 17:06:16', '2024-10-22 16:03:55', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (21, 20, '招商会门票',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/09/09/3d6bff34a07941649fb0b97f77e61726_1765x580.png', 2, 1,
        1, 1, '2024-09-09 17:06:35', '2024-10-22 16:04:03', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (22, 21, '塞浦路斯站',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/09/10/92ab5a99ffb641aba294b1d933175bb7_1765x580.png', 2, 2,
        1, 1, '2024-09-10 10:32:23', '2024-10-22 16:04:11', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (23, 21, '塞浦路斯招商会门票',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/09/10/ebb8b212b27d48719b3cdc1d9e2776f5_1765x580.png', 2, 3,
        1, 1, '2024-09-10 11:23:31', '2024-10-22 16:04:14', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (24, 0, '家用电器',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/28/51d4e8ee243240cf89b818981eb10eb8_230x230.png', 1, 20,
        0, 0, '2024-09-30 11:41:29', '2024-11-12 10:38:24', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (25, 0, '食品粮油',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/28/a351b5dfe8af4ab48529218487fcdeed_230x230.png', 1, 14,
        1, 0, '2024-09-30 11:41:51', '2024-10-28 11:52:04', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (26, 0, '3C数码',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/28/47a3f077af354ab783259702a949ae71_230x230.png', 1, 19,
        0, 0, '2024-09-30 11:49:28', '2024-11-12 10:38:21', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (27, 0, '时尚箱包',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/28/6e7c46b0be1e4a14bee4ac362d888ed8_230x230.png', 1, 25,
        0, 0, '2024-10-22 10:36:18', '2024-11-12 10:37:59', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (28, 27, '潮流女包',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/28/503b1b85150f43079f6875692c94053d_230x230.png', 2, 4,
        1, 0, '2024-10-22 11:06:20', '2024-10-28 12:15:00', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (29, 28, '单肩包',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/28/2cf2c50385fd428d91f1aaa3ef2b9247_230x230.png', 3, 8,
        1, 0, '2024-10-22 11:06:44', '2024-10-28 12:17:40', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (30, 28, '手提包',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/28/4ab8a799fa144e8db426331194314cb4_230x230.png', 3, 9,
        1, 0, '2024-10-22 11:07:03', '2024-10-28 12:17:34', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (31, 28, '斜挎包',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/28/277ade31bfca4d509e6ed752b53b7acd_230x230.png', 3, 7,
        1, 0, '2024-10-22 11:07:19', '2024-10-28 12:17:46', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (32, 28, '双肩包',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/28/a95f8228e0eb4a9a919eadd1ff0aaa10_230x230.png', 3, 6,
        1, 0, '2024-10-22 11:07:40', '2024-10-28 12:17:52', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (33, 28, '钱包',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/28/be5ba83ce65646b8b6889e99afe0028e_230x230.png', 3, 4,
        1, 0, '2024-10-22 11:07:57', '2024-10-28 12:17:57', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (34, 28, '手拿包',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/28/d516129859fa418ebf692174782e2682_230x230.png', 3, 3,
        1, 0, '2024-10-22 11:08:11', '2024-10-28 12:18:04', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (35, 28, '卡包/零钱包',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/28/4ce46c420cfb4be8ba0e039fff2f96b3_230x230.png', 3, 2,
        1, 0, '2024-10-22 11:08:26', '2024-10-28 12:18:11', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (36, 28, '钥匙包',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/28/eddc3968c144430db6b4473346be2b04_230x230.png', 3, 1,
        1, 0, '2024-10-22 11:08:41', '2024-10-28 12:18:17', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (37, 27, '精品男包',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/28/a2d9a3b5870a4ad18fc8bf973d5f108e_230x230.png', 2, 3,
        1, 0, '2024-10-22 11:12:33', '2024-10-28 12:15:09', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (38, 37, '男士钱包',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/28/b7a79a1a3b9841dd9f37c2423e617a41_230x230.png', 3, 7,
        1, 0, '2024-10-22 11:14:36', '2024-10-28 12:18:30', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (39, 37, '男士双肩包',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/28/7c7c4f28e11b4487a296afb4a7d3d98d_230x230.png', 3, 6,
        1, 0, '2024-10-22 11:15:55', '2024-10-28 12:18:35', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (40, 37, '单肩/斜挎包',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/28/e72956e98b5a4b80a3c164b64880abc0_230x230.png', 3, 5,
        1, 0, '2024-10-22 11:16:15', '2024-10-28 12:18:41', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (41, 37, '商务公文包',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/28/cc43deb9e8ac4be7831aa847488a25ef_230x230.png', 3, 4,
        1, 0, '2024-10-22 11:16:29', '2024-10-28 12:18:51', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (42, 37, '男士手包',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/28/de810ba80b0d4c85a0c6c557f97eb839_230x230.png', 3, 3,
        1, 0, '2024-10-22 11:17:12', '2024-10-28 12:18:57', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (43, 37, '卡包名片夹',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/28/b636c7c4cc0047a18679c9f7758b7438_230x230.png', 3, 2,
        1, 0, '2024-10-22 11:18:26', '2024-10-28 12:19:03', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (44, 37, '男士钥匙包',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/28/313d77f990f54e2e87486a9686426322_230x230.png', 3, 1,
        1, 0, '2024-10-22 11:18:52', '2024-10-28 12:19:09', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (45, 27, '功能箱包',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/28/894cfa734af341389f3610865d666374_230x230.png', 2, 2,
        1, 0, '2024-10-22 11:23:14', '2024-10-28 12:15:16', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (46, 45, '行李箱',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/28/0007b604864c4a988063d7ef5a0b1fb4_230x230.png', 3, 7,
        1, 0, '2024-10-22 11:23:29', '2024-10-28 12:19:18', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (47, 45, '拉杆包',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/28/4e1d6753d4fe40c2a023a3acd9b4858d_230x230.png', 3, 6,
        1, 0, '2024-10-22 11:23:45', '2024-10-28 12:19:24', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (48, 45, '旅行包',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/28/1e3896522a5d4f78903d1bbe26057db5_230x230.png', 3, 5,
        1, 0, '2024-10-22 11:24:06', '2024-10-28 12:19:30', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (49, 45, '电脑包',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/28/257c45442f1940a0acf938235f463d3b_230x230.png', 3, 4,
        1, 0, '2024-10-22 11:24:18', '2024-10-28 12:19:35', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (50, 45, '书包',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/28/4dc2524a39a7488a902e8aba90e06ecc_230x230.png', 3, 3,
        1, 0, '2024-10-22 11:24:29', '2024-10-28 12:19:40', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (51, 45, '休闲运动包',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/28/a7e610a6d5784b1aac536eb41a65699c_230x230.png', 3, 2,
        1, 0, '2024-10-22 11:24:40', '2024-10-28 12:19:45', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (52, 45, '腰包/胸包',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/28/aaf1d1221726468881d5c0fa64dad719_230x230.png', 3, 1,
        1, 0, '2024-10-22 11:24:52', '2024-10-28 12:19:51', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (53, 27, '奢侈品',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/28/32ee0e2c8dff475bbace463909f64c56_230x230.png', 2, 1,
        1, 0, '2024-10-22 11:27:42', '2024-10-28 12:15:23', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (54, 53, '箱包',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/28/b57c441df9de4950bf3576daa720edbc_230x230.png', 3, 2,
        1, 0, '2024-10-22 11:27:56', '2024-10-28 12:20:07', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (55, 53, '奢侈品钱包',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/28/1bbd68a882754139ba1b4a6424e8abad_230x230.png', 3, 1,
        1, 0, '2024-10-22 11:28:14', '2024-10-28 12:20:13', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (56, 0, '童装',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/28/b16aeff93b6b4815a71f179825c38b9a_230x230.png', 1, 10,
        0, 0, '2024-10-22 11:34:23', '2024-11-12 10:27:11', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (57, 56, '套装',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/459ce5e90d3d4d74874b910ad2711fa0_247x226.png', 2, 1,
        1, 0, '2024-10-22 11:35:21', '2024-10-22 11:35:21', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (58, 57, '睡衣',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/a574dda464bb48909f3d3935fc760429_247x226.png', 3, 1,
        1, 0, '2024-10-22 11:35:45', '2024-10-22 11:35:45', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (59, 56, '上装',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/6169252bc9de4dbb9bc086c3d114d6a1_247x226.png', 2, 2,
        1, 0, '2024-10-22 11:36:20', '2024-10-22 11:36:20', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (60, 59, '衬衫',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/21c255a3e4f44e4b94227255b1e0ccaf_247x226.png', 3, 1,
        1, 0, '2024-10-22 11:36:45', '2024-10-22 11:36:45', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (61, 59, 'T恤',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/152d3d3df34746a59da2b66fa7366a02_247x226.png', 3, 2,
        1, 0, '2024-10-22 11:37:16', '2024-10-22 11:37:16', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (62, 59, '卫衣',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/8db722ab033d409589861a0a42bf354a_247x226.png', 3, 3,
        1, 0, '2024-10-22 11:37:34', '2024-10-22 11:37:34', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (63, 59, '外套',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/16920cb638624a33b9c5a261878677d9_247x226.png', 3, 4,
        1, 0, '2024-10-22 11:38:09', '2024-10-22 11:38:09', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (64, 56, '下装',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/7cf44500882d4e6291112b2e78cf713b_247x226.png', 2, 1,
        1, 0, '2024-10-22 11:39:11', '2024-10-22 11:39:11', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (65, 64, '牛仔裤',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/17bfdf16b8874d7b9a6f6a8e20cead97_247x226.png', 3, 1,
        1, 0, '2024-10-22 11:39:33', '2024-10-22 11:40:11', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (66, 64, '休闲裤',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/afe1b3f19bb34a768ec0a96cb38226d6_247x226.png', 3, 2,
        1, 0, '2024-10-22 11:39:49', '2024-10-22 11:40:15', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (67, 0, '男装',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/28/60bdc644bb1d4b45b6fb0b491b82e090_230x230.png', 1, 12,
        1, 0, '2024-10-22 11:41:26', '2024-10-28 11:52:17', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (68, 67, '男装套装',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/a9305e82a7ad4886aece7d34ea9a2c3a_247x226.png', 2, 1,
        1, 0, '2024-10-22 11:43:02', '2024-10-22 11:43:02', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (69, 68, '男装睡衣',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/0766ef17ad794a708a051a84c22f37da_247x226.png', 3, 1,
        1, 0, '2024-10-22 11:43:28', '2024-10-22 11:43:28', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (71, 67, '男装下装',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/be8af3ca18574f9c9f005fcdd08aa3ea_247x226.png', 2, 2,
        1, 0, '2024-10-22 11:45:48', '2024-10-22 11:45:48', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (72, 71, '西裤',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/0f69c5a072e94fcea059b2ca34b7dfaf_247x226.png', 3, 1,
        1, 0, '2024-10-22 11:46:44', '2024-10-22 11:46:44', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (73, 71, '男装牛仔裤',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/3f5ad15b653b4ae2b3c8d619103c7e3f_247x226.png', 3, 2,
        1, 0, '2024-10-22 11:47:21', '2024-10-22 11:47:21', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (74, 71, '男装休闲裤',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/188a92ab571b4ed388ab0b40d82a48a2_247x226.png', 3, 3,
        1, 0, '2024-10-22 11:47:51', '2024-10-22 11:47:51', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (75, 0, '时尚配饰',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/28/3f809fa8c2064c418ae3545d89f79ca2_230x230.png', 1, 24,
        0, 0, '2024-10-22 11:49:38', '2024-11-12 10:38:01', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (76, 75, '女士配饰',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/28/6124dadb174946b4a59b526dabcaa56d_230x230.png', 2, 2,
        1, 0, '2024-10-22 11:49:56', '2024-10-28 12:15:34', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (77, 76, '腕表',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/28/6f9c599d18e241c78755d1eb42fef893_230x230.png', 3, 1,
        1, 0, '2024-10-22 11:50:12', '2024-10-28 12:17:20', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (78, 67, '男装上装',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/4a270dde851e4a28820b5f255fd80ce5_247x226.png', 2, 3,
        1, 0, '2024-10-22 11:50:20', '2024-10-22 11:50:32', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (79, 76, '饰品',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/28/757d7a9392f44172a080e1b4e2031ca4_230x230.png', 3, 2,
        1, 0, '2024-10-22 11:50:29', '2024-10-28 12:17:14', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (80, 76, '帽子',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/28/482d1bab830346bcbea07e9ea038da3a_230x230.png', 3, 3,
        1, 0, '2024-10-22 11:50:40', '2024-10-28 12:17:07', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (81, 78, '男装衬衫',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/514281ee11664a7cb6858fa8c3e0fde5_247x226.png', 3, 1,
        1, 0, '2024-10-22 11:50:50', '2024-10-22 11:50:50', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (82, 76, '太阳镜',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/28/9e71d0c1981a4bfba245ea2a6b66d9b7_230x230.png', 3, 4,
        1, 0, '2024-10-22 11:50:51', '2024-10-28 12:17:01', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (83, 76, '腰带',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/28/bb076ecc3cc14162aee1787e1527a72c_230x230.png', 3, 5,
        1, 0, '2024-10-22 11:51:04', '2024-10-28 12:16:55', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (84, 78, '男装T恤',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/516758534bc24d9997ff6468c4922663_247x226.png', 3, 2,
        1, 0, '2024-10-22 11:51:09', '2024-10-22 11:51:09', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (85, 76, '丝巾围巾',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/28/d22337aa470b472a8612ec01067300f8_230x230.png', 3, 6,
        1, 0, '2024-10-22 11:51:16', '2024-10-28 12:16:49', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (86, 78, '男装卫衣',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/33ae30f881b64edab0cb5daa9dfcf832_247x226.png', 3, 3,
        1, 0, '2024-10-22 11:51:29', '2024-10-22 11:51:29', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (88, 78, '男装外套',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/f5f5f153e33b49edb67866826080162b_247x226.png', 3, 4,
        1, 0, '2024-10-22 11:51:55', '2024-10-22 11:51:55', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (89, 75, '男士配饰',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/28/2f17e73e52b54e30a65894678a0fe593_230x230.png', 2, 1,
        1, 0, '2024-10-22 11:52:06', '2024-10-28 12:15:40', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (90, 89, '男士腕表',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/28/f8601f2b0aed48a5823c8787b53f55c8_230x230.png', 3, 1,
        1, 0, '2024-10-22 11:52:20', '2024-10-28 12:16:36', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (91, 0, '女装',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/28/1447982f7b4b42c3b650ffe727bbcf35_230x230.png', 1, 11,
        1, 0, '2024-10-22 11:52:20', '2024-10-28 11:52:23', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (92, 89, '男士饰品',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/28/bd0bd3d434164fe88ce29e5b8a4bde24_230x230.png', 3, 2,
        1, 0, '2024-10-22 11:52:34', '2024-10-28 12:16:30', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (93, 89, '男士帽子',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/28/94a16913203f4a62adb12d5fcc504abc_230x230.png', 3, 3,
        1, 0, '2024-10-22 11:52:46', '2024-10-28 12:16:24', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (94, 89, '男士太阳镜',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/28/9fbfcdc66ca44f08b6225624e27b4c78_230x230.png', 3, 4,
        1, 0, '2024-10-22 11:52:56', '2024-10-28 12:16:19', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (95, 89, '男士腰带',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/28/470341a936f04a63a68bab763b2ede05_230x230.png', 3, 5,
        1, 0, '2024-10-22 11:53:44', '2024-10-28 12:16:13', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (96, 89, '男士丝巾围巾',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/28/8eaa26df804b456e9be2a08c0086a899_230x230.png', 3, 6,
        1, 0, '2024-10-22 11:53:56', '2024-10-28 12:16:07', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (97, 0, '美妆护肤',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/28/2578b7f0fead4254b7cfc56913c55bc7_230x230.png', 1, 23,
        1, 0, '2024-10-22 11:54:27', '2024-10-28 11:51:02', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (98, 97, '香水',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/5d1626d9a989461aa350400fe09f79bd_247x226.png', 2, 1,
        1, 0, '2024-10-22 11:54:40', '2024-10-22 11:54:40', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (99, 97, '男士护理',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/b4d7eb982b6b4fdeb618fe5a465bd924_247x226.png', 2, 2,
        1, 0, '2024-10-22 11:55:08', '2024-10-22 11:55:08', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (100, 97, '魅力彩妆',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/fc5fd022a21540c4a00b9f2827f51628_247x226.png', 2, 3,
        1, 0, '2024-10-22 11:55:23', '2024-10-22 11:55:23', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (101, 97, '基础护肤',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/0aa0df727a0642fdac0672f7d709c8e5_247x226.png', 2, 4,
        1, 0, '2024-10-22 11:55:36', '2024-10-22 11:55:36', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (102, 101, '润唇膏',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/550ffa99426b4722bc364319f55effbc_247x226.png', 3, 1,
        1, 0, '2024-10-22 11:55:54', '2024-10-22 11:55:54', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (103, 101, 'T区护理',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/4d77fb5955bb4e1bb21d2b22b12189d5_247x226.png', 3, 2,
        1, 0, '2024-10-22 11:56:11', '2024-10-22 11:56:11', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (104, 101, '喷雾',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/e253ee7662d24f26a10d879374d759c3_247x226.png', 3, 3,
        1, 0, '2024-10-22 11:56:23', '2024-10-22 11:56:23', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (105, 101, 'BB/CC霜',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/36bdf1ba9ef34830adf5fd237107e101_247x226.png', 3, 4,
        1, 0, '2024-10-22 11:56:35', '2024-10-22 11:56:35', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (106, 101, '隔离/防晒',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/568289296b0a462993ed3ec2b97ab2ae_247x226.png', 3, 5,
        1, 0, '2024-10-22 11:56:50', '2024-10-22 11:56:50', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (107, 101, '化妆水',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/10231256022545659a08ead796181cf7_247x226.png', 3, 6,
        1, 0, '2024-10-22 11:57:00', '2024-10-22 11:57:00', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (108, 101, '洁面',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/58ede57c54b044b784f1e8ce5efbb5af_247x226.png', 3, 7,
        1, 0, '2024-10-22 11:57:10', '2024-10-22 11:57:10', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (109, 101, '卸妆',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/85a4802b2a254536a1cedbca8f3c271a_247x226.png', 3, 8,
        1, 0, '2024-10-22 11:57:24', '2024-10-22 11:57:24', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (110, 101, '眼霜',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/e51acf9a21294c7883e3bc1f491d338a_247x226.png', 3, 9,
        1, 0, '2024-10-22 11:57:35', '2024-10-22 11:57:35', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (111, 101, '面膜',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/52b1966057ab426ea35bd72f2b28f99a_247x226.png', 3, 10,
        1, 0, '2024-10-22 11:57:49', '2024-10-22 11:57:49', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (112, 101, '护肤套装',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/e151389558e0472f8af8d0171e4c0e4a_247x226.png', 3, 11,
        1, 0, '2024-10-22 11:58:00', '2024-10-22 11:58:00', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (113, 101, '乳液/面霜',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/09218b847c364a99881484cf028306ef_247x226.png', 3, 12,
        1, 0, '2024-10-22 11:58:12', '2024-10-22 11:58:12', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (114, 101, '精华',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/4fbd0c52d0c746fcb97f0d7651c68d88_247x226.png', 3, 13,
        1, 0, '2024-10-22 11:58:27', '2024-10-22 11:58:27', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (115, 100, '美甲',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/f50da40ec53e46fcaab8268f898fdd6d_247x226.png', 3, 1,
        1, 0, '2024-10-22 11:58:52', '2024-10-22 11:58:52', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (116, 100, '彩妆工具',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/7c3affce3b8d499b84e054a4d2a4aa9a_247x226.png', 3, 2,
        1, 0, '2024-10-22 11:59:09', '2024-10-22 11:59:09', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (117, 100, '定妆喷雾',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/100f805b29784ded8bd753996cba2539_247x226.png', 3, 3,
        1, 0, '2024-10-22 11:59:21', '2024-10-22 11:59:21', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (118, 100, '眉笔/眉粉',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/2d640554f62849c79bb38ed8346d2796_247x226.png', 3, 4,
        1, 0, '2024-10-22 11:59:33', '2024-10-22 11:59:33', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (119, 100, '眼线',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/798a6511ba894e42abc849869fa3de7e_247x226.png', 3, 5,
        1, 0, '2024-10-22 11:59:45', '2024-10-22 11:59:45', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (120, 100, '睫毛膏',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/e1e025623ee64e3db5ef55a8287485f7_247x226.png', 3, 6,
        1, 0, '2024-10-22 11:59:56', '2024-10-22 11:59:56', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (121, 100, '散粉/蜜粉',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/a59fafeab75e43048ebb3e7fe96299e4_247x226.png', 3, 7,
        1, 0, '2024-10-22 12:00:07', '2024-10-22 12:00:07', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (122, 100, '粉饼',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/2b7ec024ed4e486b80ee2f80a89a9fb6_247x226.png', 3, 8,
        1, 0, '2024-10-22 12:00:18', '2024-10-22 12:00:18', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (123, 100, '腮红',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/b221ff810b984f88bf990c9355cb3df8_247x226.png', 3, 9,
        1, 0, '2024-10-22 12:00:29', '2024-10-22 12:00:29', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (124, 100, '唇彩/唇釉',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/645f0d3ffe9e4b928b61b87e7b65dddf_247x226.png', 3, 10,
        1, 0, '2024-10-22 12:00:40', '2024-10-22 12:00:40', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (125, 100, '彩妆套装',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/232fc65640134e39b280c71fadfd7963_247x226.png', 3, 11,
        1, 0, '2024-10-22 12:00:53', '2024-10-22 12:00:53', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (126, 100, '眼影',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/34fd0d0625034c98bce2811d509a0e94_247x226.png', 3, 12,
        1, 0, '2024-10-22 12:01:11', '2024-10-22 12:01:11', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (127, 100, '妆前乳',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/93ab3ae5d79a4b39aa4e4ec89daaed68_247x226.png', 3, 13,
        1, 0, '2024-10-22 12:01:23', '2024-10-22 12:01:23', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (128, 100, '底妆',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/87136d7256b8406793141e8be215cf96_247x226.png', 3, 14,
        1, 0, '2024-10-22 12:01:33', '2024-10-22 12:01:33', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (129, 100, '遮瑕',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/a0fac52b6f3648c7b6e6ff1441afc944_247x226.png', 3, 15,
        1, 0, '2024-10-22 12:01:44', '2024-10-22 12:01:44', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (130, 100, '口红',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/a7f9180eceb149c49b5f9d0a7a068aed_247x226.png', 3, 16,
        1, 0, '2024-10-22 12:01:54', '2024-10-22 12:01:54', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (131, 99, '套装/礼盒',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/1824b749ecaa437eb28a2df41501a29b_247x226.png', 3, 1,
        1, 0, '2024-10-22 12:02:18', '2024-10-22 12:02:18', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (132, 99, '防晒',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/46564c3618b44af99e3ae5fd24a55bd3_247x226.png', 3, 2,
        1, 0, '2024-10-22 12:02:33', '2024-10-22 12:02:33', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (133, 99, '男士精华',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/b35e1696bacc40d79828cafc2a21a0b8_247x226.png', 3, 3,
        1, 0, '2024-10-22 12:02:51', '2024-10-22 12:02:51', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (134, 99, '爽肤水',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/4c179dccf4014b479519e6e7e874ea70_247x226.png', 3, 4,
        1, 0, '2024-10-22 12:03:04', '2024-10-22 12:03:04', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (135, 99, '男士面膜',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/b75d187c5bd94c84825b9dacf4b7ace9_247x226.png', 3, 5,
        1, 0, '2024-10-22 12:03:20', '2024-10-22 12:03:20', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (136, 99, '男士乳液/面霜',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/b8f88c862fe346ac9c94c3fe12b9635e_247x226.png', 3, 6,
        1, 0, '2024-10-22 12:03:37', '2024-10-22 12:03:37', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (137, 99, '男士洁面',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/7790a7e2ebb04e9bb37f0944f22655ee_247x226.png', 3, 7,
        1, 0, '2024-10-22 12:03:52', '2024-10-22 12:03:52', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (138, 98, '香水套装',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/142fbfb7126b4a19b80d386c7e5950f9_247x226.png', 3, 1,
        1, 0, '2024-10-22 12:04:12', '2024-10-22 12:04:12', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (139, 98, '女士香水',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/1b543dc47a5d4b9cbc3bc8e3c90ad293_247x226.png', 3, 2,
        1, 0, '2024-10-22 12:04:27', '2024-10-22 12:04:27', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (140, 98, '男士香水',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/fb628a157adc4fc8a02ef2cbc468648b_247x226.png', 3, 3,
        1, 0, '2024-10-22 12:04:38', '2024-10-22 12:04:38', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (141, 0, '个护清洁',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/28/2114498167974d4db0ad0d1f9d14b4e4_230x230.png', 1, 22,
        1, 0, '2024-10-22 12:05:03', '2024-10-28 11:51:09', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (142, 141, '女性护理',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/693abd3291fb413894d5cee2e7f180b1_247x226.png', 2, 1,
        1, 0, '2024-10-22 12:05:20', '2024-10-22 12:05:20', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (143, 141, '身体护理',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/2f250c4abc5f49d09ab67dec1b18bb54_247x226.png', 2, 2,
        1, 0, '2024-10-22 12:05:33', '2024-10-22 12:05:33', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (144, 141, '口腔护理',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/f74541c6c78143c8bac018dc9896b027_247x226.png', 2, 3,
        1, 0, '2024-10-22 12:05:45', '2024-10-22 12:05:45', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (145, 141, '洗发护法',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/33dbd91e08184f0f8cf74dcb7562759b_247x226.png', 2, 4,
        1, 0, '2024-10-22 12:05:55', '2024-10-22 12:05:55', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (146, 145, '染发',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/47921b9261e64127bd55843a1164e458_247x226.png', 3, 1,
        1, 0, '2024-10-22 12:06:18', '2024-10-22 12:06:18', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (147, 145, '洗护套装',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/d366f076713e4f7081c872a58cfca294_247x226.png', 3, 2,
        1, 0, '2024-10-22 12:06:34', '2024-10-22 12:06:34', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (148, 145, '发膜/精油',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/a29f09b258a34165ab4d805408e8b2a9_247x226.png', 3, 3,
        1, 0, '2024-10-22 12:06:47', '2024-10-22 12:06:47', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (149, 145, '护发素',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/bd80f7967add478293ed337ce992f9dd_247x226.png', 3, 4,
        1, 0, '2024-10-22 12:06:57', '2024-10-22 12:06:57', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (150, 145, '洗发水',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/55785aed86e5464eb6e71cd3fedb38c0_247x226.png', 3, 5,
        1, 0, '2024-10-22 12:07:06', '2024-10-22 12:07:06', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (151, 144, '口腔护理套装',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/a805f8d559094722aa350080dbb814d8_247x226.png', 3, 1,
        1, 0, '2024-10-22 12:07:42', '2024-10-22 12:07:42', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (152, 144, '口喷',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/3c0ffa38a3ca4e23b2f82f0e05b52c23_247x226.png', 3, 2,
        1, 0, '2024-10-22 12:07:58', '2024-10-22 12:07:58', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (153, 144, '漱口水',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/fcdc52568b544bf186652843fdf31644_247x226.png', 3, 3,
        1, 0, '2024-10-22 12:08:07', '2024-10-22 12:08:07', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (154, 144, '牙线',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/19c2f2cf3e624e9e9b72b90cd4aed5b0_247x226.png', 3, 4,
        1, 0, '2024-10-22 12:08:17', '2024-10-22 12:08:17', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (155, 144, '牙刷',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/2f5f650faeb946da9e672a327824346e_247x226.png', 3, 5,
        1, 0, '2024-10-22 12:08:26', '2024-10-22 12:08:26', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (156, 144, '牙膏',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/35fdac5f1d0c43cfbce7a89c8dfa71ab_247x226.png', 3, 6,
        1, 0, '2024-10-22 12:08:36', '2024-10-22 12:08:36', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (157, 143, '花露水',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/6dfa7ad4f0ad4f33b2fa1cc4d465b70e_247x226.png', 3, 1,
        1, 0, '2024-10-22 12:09:02', '2024-10-22 12:09:02', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (158, 143, '脱毛刀/膏',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/3b6b64ec6f824966a2cdb3d0caede422_247x226.png', 3, 2,
        1, 0, '2024-10-22 12:09:16', '2024-10-22 12:09:16', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (159, 143, '走珠/止汗露',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/0b8a09d93c994cee99ea9a945cf4fd32_247x226.png', 3, 3,
        1, 0, '2024-10-22 12:09:29', '2024-10-22 12:09:29', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (160, 143, '润肤/精油',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/2f2ab838aa19451f892f025d1953dd85_247x226.png', 3, 4,
        1, 0, '2024-10-22 12:09:41', '2024-10-22 12:09:41', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (161, 143, '护手霜',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/8bbe9a39ee634722b4f4f6bc56973929_247x226.png', 3, 5,
        1, 0, '2024-10-22 12:09:53', '2024-10-22 12:09:53', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (162, 143, '洗手液',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/9e630f388beb46c8a2784ea00464ceef_247x226.png', 3, 6,
        1, 0, '2024-10-22 12:10:03', '2024-10-22 12:10:03', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (163, 143, '香皂',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/5c076f631b4946b3b03366cdca3398ba_247x226.png', 3, 7,
        1, 0, '2024-10-22 12:10:15', '2024-10-22 12:10:15', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (164, 143, '沐浴露',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/b1d2b913f12144e488b84a0d35f7652a_247x226.png', 3, 8,
        1, 0, '2024-10-22 12:10:25', '2024-10-22 12:10:25', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (165, 142, '卫生护垫',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/9e551c4599654030a4d2e5814c175d50_247x226.png', 3, 1,
        1, 0, '2024-10-22 12:10:44', '2024-10-22 12:10:44', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (166, 142, '卫生棉条',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/9b33be62c1ec4642aa674396b8891591_247x226.png', 3, 2,
        1, 0, '2024-10-22 12:10:56', '2024-10-22 12:10:56', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (167, 142, '卫生巾',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/1f8136c5de4b4353ab62d5b10d4aed13_247x226.png', 3, 3,
        1, 0, '2024-10-22 12:11:07', '2024-10-22 12:11:07', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (168, 0, '运动户外',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/28/c25d182c9cee44079d1d9d695fe24871_230x230.png', 1, 21,
        0, 0, '2024-10-22 12:11:40', '2024-11-12 10:38:26', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (169, 168, '游泳用品',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/2cb3765d0e15468eb6428fa62e2ef8aa_247x226.png', 2, 1,
        1, 0, '2024-10-22 12:11:54', '2024-10-22 12:11:54', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (170, 168, '户外装备',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/eba969effc0f432598a49e27a8969565_247x226.png', 2, 2,
        1, 0, '2024-10-22 12:12:11', '2024-10-22 12:12:11', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (171, 168, '户外鞋服',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/9865b19b9b574c4bafa43a0e4959b540_247x226.png', 2, 3,
        1, 0, '2024-10-22 12:12:26', '2024-10-22 12:12:26', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (172, 168, '体育用品',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/174c5298de87441ba9834a6ed314d3d4_247x226.png', 2, 4,
        1, 0, '2024-10-22 12:12:40', '2024-10-22 12:12:40', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (173, 168, '骑行运动',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/d066f06e3dbc43f4a282941dcab3e320_247x226.png', 2, 5,
        1, 0, '2024-10-22 12:12:53', '2024-10-22 12:12:53', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (174, 168, '运动鞋包',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/05033090eb344dde977f132e1965d500_247x226.png', 2, 6,
        1, 0, '2024-10-22 12:13:08', '2024-10-22 12:13:08', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (175, 168, '运动服饰',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/c04a8f3073ca4005832b17d38ea5a516_247x226.png', 2, 7,
        1, 0, '2024-10-22 12:13:20', '2024-10-22 12:13:20', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (176, 169, '游泳用品及其他',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/bd0ec090198d4b6e8dab9697eede40fa_247x226.png', 3, 1,
        1, 0, '2024-10-22 12:13:56', '2024-10-22 12:13:56', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (177, 169, '男士泳衣',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/14301500a7a24d16a14347fe63baac8c_247x226.png', 3, 2,
        1, 0, '2024-10-22 12:14:18', '2024-10-22 12:14:18', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (178, 169, '女士泳衣',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/527ed434cb7f40fba40e9ac07cc56b48_247x226.png', 3, 3,
        1, 0, '2024-10-22 12:14:31', '2024-10-22 12:14:31', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (179, 170, '便携桌椅床',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/304ee02b61ab4e87b75b8a8bde35d441_247x226.png', 3, 1,
        1, 0, '2024-10-22 12:14:49', '2024-10-22 12:14:49', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (180, 170, '旅行装备',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/afbd1913df2147078c4619e9e34e9486_247x226.png', 3, 2,
        1, 0, '2024-10-22 12:15:05', '2024-10-22 12:15:05', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (181, 170, '野餐用品',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/fddfb8b2deda4e6f9ecfb9c9738b0af7_247x226.png', 3, 3,
        1, 0, '2024-10-22 12:15:18', '2024-10-22 12:15:18', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (182, 170, '烧烤用具',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/31e7eefde08548fab917833d9112862f_247x226.png', 3, 4,
        1, 0, '2024-10-22 12:15:31', '2024-10-22 12:15:31', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (183, 170, '帐篷/垫子',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/2635ee40bd924f588b3943ef012f5bb8_247x226.png', 3, 5,
        1, 0, '2024-10-22 12:15:42', '2024-10-22 12:15:42', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (185, 171, '登山鞋',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/1b1049e8ab664c37a546301a1ae5ca8c_247x226.png', 3, 1,
        1, 0, '2024-10-22 12:16:35', '2024-10-22 12:16:35', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (186, 171, '户外防晒衣',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/415e86ef1f5446fa848771fc72b71126_247x226.png', 3, 2,
        1, 0, '2024-10-22 12:16:51', '2024-10-22 12:16:51', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (187, 171, '户外风衣',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/17e253a754624e638b64062e6e2c9884_247x226.png', 3, 3,
        1, 0, '2024-10-22 12:17:01', '2024-10-22 12:17:01', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (188, 172, '高尔夫',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/edc7379662454dd992e1fa38842d5125_247x226.png', 3, 1,
        1, 0, '2024-10-22 12:17:17', '2024-10-22 12:17:17', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (189, 172, '网球',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/a36b9ef747dd4d579dac0f336ddfcb53_247x226.png', 3, 2,
        1, 0, '2024-10-22 12:17:32', '2024-10-22 12:17:32', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (190, 172, '轮滑滑板',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/162ad8fa4aea47559434118dafe2c80a_247x226.png', 3, 3,
        1, 0, '2024-10-22 12:17:43', '2024-10-22 12:17:43', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (191, 172, '足球',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/3f111aae6aad46a7aa72dfafb974a5ac_247x226.png', 3, 4,
        1, 0, '2024-10-22 12:17:53', '2024-10-22 12:17:53', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (192, 172, '篮球',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/58919d4634234d4ba5acb95b9c539cda_247x226.png', 3, 5,
        1, 0, '2024-10-22 12:18:03', '2024-10-22 12:18:03', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (193, 172, '羽毛球',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/41ed4aced1104cda94788537a1dde3ba_247x226.png', 3, 6,
        1, 0, '2024-10-22 12:18:14', '2024-10-22 12:18:14', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (194, 172, '乒乓球',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/e581f72cf1b24111905cc74e992ad775_247x226.png', 3, 7,
        1, 0, '2024-10-22 12:18:55', '2024-10-22 12:18:55', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (195, 173, '城市自行车',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/d14e6d85389d48838e03487b55071a02_247x226.png', 3, 1,
        1, 0, '2024-10-22 12:19:20', '2024-10-22 12:19:20', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (196, 173, '折叠车',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/dcd725e83a3f45628b7ce8c1d03aee7a_247x226.png', 3, 2,
        1, 0, '2024-10-22 12:19:34', '2024-10-22 12:19:34', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (197, 173, '山地车',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/22c6dcd1dc2d4059b2d5cfd8004ad5c9_247x226.png', 3, 3,
        1, 0, '2024-10-22 12:19:45', '2024-10-22 12:19:45', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (199, 174, '运动包',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/146281df12b4450f9a5f14d41692a7c2_247x226.png', 3, 1,
        1, 0, '2024-10-22 12:20:27', '2024-10-22 12:20:27', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (200, 174, '拖鞋',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/735f55214b6e450b8ce17b8f609a528c_247x226.png', 3, 2,
        1, 0, '2024-10-22 12:20:47', '2024-10-22 12:20:47', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (201, 174, '板鞋',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/30e792949e044e58bd4a68d0fd07929b_247x226.png', 3, 3,
        1, 0, '2024-10-22 12:20:59', '2024-10-22 12:20:59', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (202, 174, '篮球鞋',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/a1c6a1018b49485ca5dd55465d30466c_247x226.png', 3, 4,
        1, 0, '2024-10-22 12:21:11', '2024-10-22 12:21:11', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (203, 174, '休闲鞋',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/3b54f49f492e49859ba1ae8e5704ef2a_247x226.png', 3, 5,
        1, 0, '2024-10-22 12:21:25', '2024-10-22 12:21:25', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (204, 174, '跑步鞋',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/b49fa2f094f9469a8c7a09b56812548c_247x226.png', 3, 6,
        1, 0, '2024-10-22 12:21:51', '2024-10-22 12:21:51', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (205, 175, '棉服',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/efbc25f1e06847279abc2d55d15a55d8_247x226.png', 3, 1,
        1, 0, '2024-10-22 12:22:13', '2024-10-22 12:22:13', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (206, 175, '夹克风衣',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/92fdd3b8607e4f088bce7b1bb06905bb_247x226.png', 3, 2,
        1, 0, '2024-10-22 12:22:28', '2024-10-22 12:22:28', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (207, 175, '运动套装',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/1075ddc372a846fe86cdd96c9dfed823_247x226.png', 3, 3,
        1, 0, '2024-10-22 12:22:42', '2024-10-22 12:22:42', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (208, 175, '运动裤',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/e3582d5ff4d647a6bbfe0e6b642f0a7e_247x226.png', 3, 4,
        1, 0, '2024-10-22 12:22:54', '2024-10-22 12:22:54', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (209, 175, '卫衣/套头衫',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/d01cfcfd2392485099dc6519eace70e6_247x226.png', 3, 5,
        1, 0, '2024-10-22 12:23:10', '2024-10-22 12:23:10', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (210, 175, '运动T恤',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/1bfd1ac6e7c24a2586eaf994d42837b1_247x226.png', 3, 6,
        1, 0, '2024-10-22 12:23:31', '2024-10-22 12:23:31', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (211, 26, '智能设备',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/1b4fc36567f742c883275be7da5da469_247x226.png', 2, 1,
        1, 0, '2024-10-22 12:24:26', '2024-10-22 12:24:26', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (212, 26, '暴音娱乐',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/1f9a63c811dc45d08bc541310607e3c8_247x226.png', 2, 2,
        1, 0, '2024-10-22 12:24:39', '2024-10-22 12:24:39', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (213, 26, '摄影摄像',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/8a825148b41a4c55937862928e98f7bf_247x226.png', 2, 3,
        1, 0, '2024-10-22 12:24:53', '2024-10-22 12:24:53', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (214, 26, '手机',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/6e030e8e2af546f1ab10e93caf3218e4_247x226.png', 2, 4,
        1, 0, '2024-10-22 12:25:04', '2024-10-22 12:25:04', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (215, 214, '折叠屏手机',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/710eaf7ff54c47f0861a79fbd2b44667_247x226.png', 3, 1,
        1, 0, '2024-10-22 12:25:16', '2024-10-22 12:25:16', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (216, 214, '拍照手机',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/380d2cba33b947b4b99ba8b41d5fbfab_247x226.png', 3, 2,
        1, 0, '2024-10-22 12:25:36', '2024-10-22 12:25:36', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (217, 213, '摄像机',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/38d01a5b8d9e4a88953f4bbf63bc54a4_247x226.png', 3, 1,
        1, 0, '2024-10-22 12:26:43', '2024-10-22 12:26:43', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (218, 213, '拍立得',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/f3f109b6800841ea92c15435c2350bca_247x226.png', 3, 2,
        1, 0, '2024-10-22 12:27:00', '2024-10-22 12:27:00', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (219, 213, '单反相机',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/f7ce6b0a9f8a4b75a4aeede3194e8fa3_247x226.png', 3, 3,
        1, 0, '2024-10-22 12:27:11', '2024-10-22 12:27:11', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (220, 213, '微单相机',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/985d3d550f814735b935bf60c5764277_247x226.png', 3, 4,
        1, 0, '2024-10-22 12:27:21', '2024-10-22 12:27:21', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (221, 213, '数码相机',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/1f9ac025e87e497e8f2ae569dfb951e8_247x226.png', 3, 5,
        1, 0, '2024-10-22 12:27:32', '2024-10-22 12:27:32', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (222, 212, '音箱/音响',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/df56f706300841a1afa6a74865412068_247x226.png', 3, 1,
        1, 0, '2024-10-22 12:27:59', '2024-10-22 12:27:59', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (223, 212, '蓝牙/无线耳机',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/347b1963e1fb47cf83a5c72e9c2ffcc4_247x226.png', 3, 2,
        1, 0, '2024-10-22 12:28:12', '2024-10-22 12:28:12', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (224, 211, '智能手表',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/4f715fff9f8d44e3a11a20742b98e9d7_247x226.png', 3, 1,
        1, 0, '2024-10-22 12:28:25', '2024-10-22 12:28:25', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (225, 24, '冰箱',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/022b8deedced4bc591e433b90f2985be_247x226.png', 2, 1,
        1, 0, '2024-10-22 12:29:15', '2024-10-22 12:29:15', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (226, 24, '洗衣机',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/60f762c02be946749424001647ad7d8b_247x226.png', 2, 2,
        1, 0, '2024-10-22 12:29:52', '2024-10-22 12:29:52', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (227, 24, '空调',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/adeeaae14b434e538d98af871b74a8a3_247x226.png', 2, 3,
        1, 0, '2024-10-22 12:30:05', '2024-10-22 12:30:05', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (228, 24, '电视',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/f0bf71c3c63549d696e7726f4227c8ad_247x226.png', 2, 4,
        1, 0, '2024-10-22 12:30:18', '2024-10-22 12:30:18', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (229, 228, '100寸电视',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/a86f0b97055d4f578bd81874cc74a33e_247x226.png', 3, 1,
        1, 0, '2024-10-22 13:59:40', '2024-10-22 13:59:40', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (230, 228, '98寸电视',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/819bff4a6e41479aad93e0755202a169_247x226.png', 3, 2,
        1, 0, '2024-10-22 13:59:56', '2024-10-22 13:59:56', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (231, 228, '85寸电视',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/fceda93ed149471e9bd8c00028ce0898_247x226.png', 3, 3,
        1, 0, '2024-10-22 14:00:07', '2024-10-22 14:00:07', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (232, 228, '75寸电视',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/01b977003e594b75b9c496defcc178e5_247x226.png', 3, 4,
        1, 0, '2024-10-22 14:00:17', '2024-10-22 14:00:17', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (233, 228, '会议电视',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/43dbee51065d4994a4406c3f704521b1_247x226.png', 3, 5,
        1, 0, '2024-10-22 14:00:34', '2024-10-22 14:00:34', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (234, 228, '游戏电视',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/f8b108adf4ed4aa5a6075f46e1c2eab3_247x226.png', 3, 6,
        1, 0, '2024-10-22 14:00:46', '2024-10-22 14:00:46', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (235, 227, '中央空调',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/e08d0a9869364a2bbfbc105c4783f8f5_247x226.png', 3, 1,
        1, 0, '2024-10-22 14:01:09', '2024-10-22 14:01:09', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (236, 227, '变频空调',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/59aabc7e24b94ff392928446c94e40c4_247x226.png', 3, 2,
        1, 0, '2024-10-22 14:01:27', '2024-10-22 14:01:27', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (237, 227, '空调柜机',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/338dee774f794b1b9c3ab838cc67a9f2_247x226.png', 3, 3,
        1, 0, '2024-10-22 14:01:38', '2024-10-22 14:01:38', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (238, 227, '空调挂机',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/457a4a9a8bb3476d99110f81cd0d2e12_247x226.png', 3, 4,
        1, 0, '2024-10-22 14:01:48', '2024-10-22 14:01:48', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (239, 226, '迷你洗衣机',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/a3656707c2384611b0a58522dcfbac2e_247x226.png', 3, 1,
        1, 0, '2024-10-22 14:02:05', '2024-10-22 14:02:05', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (240, 226, '波轮洗衣机',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/af14b2e651934a198299bf6efec84374_247x226.png', 3, 2,
        1, 0, '2024-10-22 14:02:17', '2024-10-22 14:02:17', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (241, 226, '洗烘一体机',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/d7ba07b5421643dc9ee3c47ba1967b14_247x226.png', 3, 3,
        1, 0, '2024-10-22 14:02:28', '2024-10-22 14:02:28', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (242, 226, '滚筒洗衣机',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/370d953361fc4546819f37be7981b3c5_247x226.png', 3, 4,
        1, 0, '2024-10-22 14:02:39', '2024-10-22 14:02:39', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (243, 225, '酒柜',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/c88717853f9a4700962fb06e7ff37375_247x226.png', 3, 1,
        1, 0, '2024-10-22 14:02:54', '2024-10-22 14:02:54', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (244, 225, '双门',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/2c90013d2046467c8ab52ed928be1cc0_247x226.png', 3, 2,
        1, 0, '2024-10-22 14:03:09', '2024-10-22 14:03:09', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (245, 225, '三门',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/2391583e18004ca2a181a3a098556af6_247x226.png', 3, 3,
        1, 0, '2024-10-22 14:03:19', '2024-10-22 14:03:19', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (246, 225, '对开门',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/e5865e5423394ac68c17c65aed0008ac_247x226.png', 3, 4,
        1, 0, '2024-10-22 14:03:29', '2024-10-22 14:03:29', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (247, 225, '多门',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/accee6dad020409a8eca4d18e62781d7_247x226.png', 3, 5,
        1, 0, '2024-10-22 14:03:38', '2024-10-22 14:03:38', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (248, 0, '家居厨具',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/28/538a1245b8f24d58b4921eeed765c67e_230x230.png', 1, 18,
        0, 0, '2024-10-22 14:04:22', '2024-11-12 10:38:20', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (249, 248, '床上用品',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/231b4810bd744ead8cf7a27ae993d7de_247x226.png', 2, 2,
        1, 0, '2024-10-22 14:05:13', '2024-10-22 14:05:13', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (250, 248, '厨具',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/a93d7e72983a40259b93ee42ce141413_247x226.png', 2, 3,
        1, 0, '2024-10-22 14:05:26', '2024-10-22 14:05:26', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (251, 249, '床笠',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/a5a675a96b4448068a8910f944e1c7e2_247x226.png', 3, 1,
        1, 0, '2024-10-22 14:06:18', '2024-10-22 14:06:18', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (252, 249, '床单',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/8d2a14bfb9a74454aa46b66acd7ba35a_247x226.png', 3, 2,
        1, 0, '2024-10-22 14:06:33', '2024-10-22 14:06:33', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (253, 249, '被套',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/d012abd53698400281c995a0bd20c269_247x226.png', 3, 3,
        1, 0, '2024-10-22 14:06:43', '2024-10-22 14:06:43', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (254, 249, '电热毯',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/d00d8084513f43c5a39fc6e7c61071c9_247x226.png', 3, 4,
        1, 0, '2024-10-22 14:06:53', '2024-10-22 14:06:53', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (255, 249, '毛毯',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/3a83c8310faa48b98fc8c4c1ba75323e_247x226.png', 3, 5,
        1, 0, '2024-10-22 14:07:03', '2024-10-22 14:07:03', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (256, 249, '枕头',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/350be298d0a34d259b5758dc7e7b3149_247x226.png', 3, 6,
        1, 0, '2024-10-22 14:07:13', '2024-10-22 14:07:13', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (257, 249, '被子',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/119d488db2dc48d08e2c9e01a0f5886f_247x226.png', 3, 7,
        1, 0, '2024-10-22 14:07:25', '2024-10-22 14:07:25', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (258, 249, '床品四件套',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/50a8d89ba9ee4abeaf5c7c13bc8c2ff9_247x226.png', 3, 8,
        1, 0, '2024-10-22 14:07:35', '2024-10-22 14:07:35', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (259, 250, '刀剪菜板',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/fdfa4de5f6a04daba6a1ace8b3e50d03_247x226.png', 3, 1,
        1, 0, '2024-10-22 14:07:52', '2024-10-22 14:07:52', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (260, 250, '厨房储物',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/85429a966f4b4fb1bdc4da8aa1111a80_247x226.png', 3, 2,
        1, 0, '2024-10-22 14:08:09', '2024-10-22 14:08:09', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (261, 250, '碗碟',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/5679323b40f6497f8a415415aa2b6c20_247x226.png', 3, 3,
        1, 0, '2024-10-22 14:08:21', '2024-10-22 14:08:21', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (262, 250, '炒锅',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/4475876a017e470690a4986327b01c3c_247x226.png', 3, 4,
        1, 0, '2024-10-22 14:08:31', '2024-10-22 14:08:31', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (263, 250, '烹饪锅具',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/ba17052474374846a41a8d091e1944fb_247x226.png', 3, 5,
        1, 0, '2024-10-22 14:08:41', '2024-10-22 14:08:41', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (264, 250, '水具酒具',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/8d7be18eeef74576aa66ff548ae118d1_247x226.png', 3, 6,
        1, 0, '2024-10-22 14:08:54', '2024-10-22 14:08:54', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (265, 0, '图书办公',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/28/71737c711fa743dfb2b7e6333ab19d7f_230x230.png', 1, 17,
        0, 0, '2024-10-22 14:09:15', '2024-11-12 10:38:18', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (266, 265, '经管励志',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/3be9b18602ab4f56925afc237f171576_247x226.png', 2, 1,
        1, 0, '2024-10-22 14:09:29', '2024-10-22 14:09:29', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (267, 265, '童书',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/a2d7493391dd4a02941329ba2335fd9d_247x226.png', 2, 2,
        1, 0, '2024-10-22 14:09:43', '2024-10-22 14:09:43', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (268, 265, '文学',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/a627c24beca246af83e2d9ce5977a6e0_247x226.png', 2, 3,
        1, 0, '2024-10-22 14:09:53', '2024-10-22 14:09:53', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (269, 265, '本册纸签',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/f1fa90a9be794d0191ff101306a8e6f4_247x226.png', 2, 4,
        1, 0, '2024-10-22 14:10:07', '2024-10-22 14:10:07', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (270, 265, '书写工具',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/1f99962ca28c43e28135e65ebc33716a_247x226.png', 2, 5,
        1, 0, '2024-10-22 14:10:18', '2024-10-22 14:10:18', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (271, 265, '财务办公',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/6499a79dfd404970bd1d76f683892d90_247x226.png', 2, 6,
        1, 0, '2024-10-22 14:10:28', '2024-10-22 14:10:28', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (272, 265, '耗材',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/8b70cde394404d59a245358a88a8940e_247x226.png', 2, 7,
        1, 0, '2024-10-22 14:10:42', '2024-10-22 14:10:42', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (273, 265, '办公设备',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/1992a21222a94ac4a4378119c0658b98_247x226.png', 2, 8,
        1, 0, '2024-10-22 14:10:54', '2024-10-22 14:10:54', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (274, 273, '保险柜',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/2a477b429d944dc4a250f58da204c50a_247x226.png', 3, 1,
        1, 0, '2024-10-22 14:11:05', '2024-10-22 14:11:05', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (275, 273, '考勤门禁',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/eef34fe5b0e84982b60ece5fd4d3dd6e_247x226.png', 3, 2,
        1, 0, '2024-10-22 14:11:22', '2024-10-22 14:11:22', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (276, 273, '复合机碎纸机',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/693cb8a62e0343ecbdf7f5c5ab2d4137_247x226.png', 3, 3,
        1, 0, '2024-10-22 14:11:33', '2024-10-22 14:11:33', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (277, 273, '打印机',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/2614cdb9b4db48d09a6cf69ba865ca31_247x226.png', 3, 4,
        1, 0, '2024-10-22 14:11:48', '2024-10-22 14:11:48', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (278, 273, '投影机',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/3c230ca12bab40ee88ef3ba1c8c5c391_247x226.png', 3, 5,
        1, 0, '2024-10-22 14:12:00', '2024-10-22 14:12:00', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (279, 272, '纸类',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/765a8ee7d32d4e9f9b040698c8619389_247x226.png', 3, 1,
        1, 0, '2024-10-22 14:12:21', '2024-10-22 14:12:21', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (280, 272, '硒鼓/墨粉',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/c13b8cb6e7f6412a8bba03871d7576c2_247x226.png', 3, 2,
        1, 0, '2024-10-22 14:12:35', '2024-10-22 14:12:35', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (281, 271, '计算器',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/08fb397fc7de4ee29408260343a20698_247x226.png', 3, 1,
        1, 0, '2024-10-22 14:12:53', '2024-10-22 14:12:53', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (282, 271, '胶水',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/f4975e8fcc454d4b85a3dfd0b6113d5c_247x226.png', 3, 2,
        1, 0, '2024-10-22 14:13:10', '2024-10-22 14:13:10', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (283, 271, '固体胶',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/6f18d9e564d6438e9561b1aace68d694_247x226.png', 3, 3,
        1, 0, '2024-10-22 14:13:20', '2024-10-22 14:13:20', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (284, 271, '透明胶带',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/58b7578b130142c083fb6460d3ee31bf_247x226.png', 3, 4,
        1, 0, '2024-10-22 14:13:30', '2024-10-22 14:13:30', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (285, 271, '双面胶带',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/9afd9bcc000f4180bf7153c33b8789c9_247x226.png', 3, 5,
        1, 0, '2024-10-22 14:13:42', '2024-10-22 14:13:42', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (286, 271, '美工刀',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/8b1299adbf9b4813a9b8e4468e1d7b4a_247x226.png', 3, 6,
        1, 0, '2024-10-22 14:13:52', '2024-10-22 14:13:52', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (287, 271, '办公学生剪刀',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/bafb41ff4b11435bb9425b4c6ef83650_247x226.png', 3, 7,
        1, 0, '2024-10-22 14:14:06', '2024-10-22 14:14:06', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (288, 271, '磁扣/磁钉',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/4667c8ee44c64f63b039bc3ce4ca239d_247x226.png', 3, 8,
        1, 0, '2024-10-22 14:14:17', '2024-10-22 14:14:17', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (289, 271, '大头针',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/50f7692fb7d64830a4e3c11cd8b8c756_247x226.png', 3, 9,
        1, 0, '2024-10-22 14:14:27', '2024-10-22 14:14:27', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (290, 271, '票夹/长尾夹',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/f08db68e809c46a596ddb1b7abe0fabf_247x226.png', 3, 10,
        1, 0, '2024-10-22 14:14:37', '2024-10-22 14:14:37', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (291, 271, '订书机',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/9a0bdd80e54843f9819243354b8c4bfa_247x226.png', 3, 11,
        1, 0, '2024-10-22 14:14:49', '2024-10-22 14:14:49', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (292, 271, '打孔机',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/6322d3dc57c74e73bbb8267f588f1e75_247x226.png', 3, 12,
        1, 0, '2024-10-22 14:14:59', '2024-10-22 14:14:59', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (293, 271, '三针一订盒/筒',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/08bf13f9590548b995914fc438e202da_247x226.png', 3, 13,
        1, 0, '2024-10-22 14:15:09', '2024-10-22 14:15:09', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (294, 270, '笔',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/18a381bfce9747df9f6b64ab358614aa_247x226.png', 3, 1,
        1, 0, '2024-10-22 14:15:39', '2024-10-22 14:15:39', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (295, 269, '便签',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/5713ba9d9a49466199965c6b31fa694d_247x226.png', 3, 1,
        1, 0, '2024-10-22 14:15:54', '2024-10-22 14:15:54', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (296, 269, '本',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/6880086fa76d444493a90135fdb1abc3_247x226.png', 3, 2,
        1, 0, '2024-10-22 14:16:07', '2024-10-22 14:16:07', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (297, 268, '世界名著',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/f46f18d4a33b4cb1aabca4b5bc274325_247x226.png', 3, 1,
        1, 0, '2024-10-22 14:16:28', '2024-10-22 14:16:28', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (298, 268, '武侠',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/17875c4e5ab74bfeaf997bc862ce1b47_247x226.png', 3, 2,
        1, 0, '2024-10-22 14:16:41', '2024-10-22 14:16:41', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (299, 268, '科幻',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/e377211755a84d889419627ea93fd00c_247x226.png', 3, 3,
        1, 0, '2024-10-22 14:16:52', '2024-10-22 14:16:52', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (300, 268, '悬疑推理',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/7f5afa952fb64945b798862ed699710a_247x226.png', 3, 4,
        1, 0, '2024-10-22 14:17:27', '2024-10-22 14:17:27', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (301, 268, '动漫',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/37ceaaaade174dd78dfc5dd411c848a7_247x226.png', 3, 5,
        1, 0, '2024-10-22 14:17:41', '2024-10-22 14:17:41', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (302, 268, '传记',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/91ab8205eb8b44be8608164e90fc37ee_247x226.png', 3, 6,
        1, 0, '2024-10-22 14:17:53', '2024-10-22 14:17:53', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (303, 268, '青春文学',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/f1dde6bbe420401f9e36d918e3167dcb_247x226.png', 3, 7,
        1, 0, '2024-10-22 14:18:04', '2024-10-22 14:18:04', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (304, 268, '散文随笔',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/f921e03eeb0b441397f4557c2ee1c884_247x226.png', 3, 8,
        1, 0, '2024-10-22 14:18:15', '2024-10-22 14:18:15', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (305, 268, '小说',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/bbca57dbab1443ecb0589cf93db6fd20_247x226.png', 3, 9,
        1, 0, '2024-10-22 14:18:25', '2024-10-22 14:18:25', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (306, 267, '玩具书',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/4c30a71ca8a842cfbc5a40fb9cfb29f9_247x226.png', 3, 1,
        1, 0, '2024-10-22 14:18:47', '2024-10-22 14:18:47', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (307, 267, '智力开发',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/08c247ac41e34ff6b2e3f9ff011bd670_247x226.png', 3, 2,
        1, 0, '2024-10-22 14:19:47', '2024-10-22 14:19:47', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (308, 267, '幼儿启蒙',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/da80640892ba4aba975ac95de9a39aff_247x226.png', 3, 3,
        1, 0, '2024-10-22 14:19:58', '2024-10-22 14:19:58', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (309, 267, '科普百科',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/8d3c89dd63d4485e81e89162a073f45b_247x226.png', 3, 4,
        1, 0, '2024-10-22 14:20:07', '2024-10-22 14:20:07', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (310, 267, '绘本',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/8320f0ce508843caae0c58324bb6f6f8_247x226.png', 3, 5,
        1, 0, '2024-10-22 14:20:16', '2024-10-22 14:20:16', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (311, 267, '儿童文学',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/209610c4a93e48f4902466f25a0ff653_247x226.png', 3, 6,
        1, 0, '2024-10-22 14:20:27', '2024-10-22 14:20:27', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (312, 266, '市场营销',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/d29f2019661046dab5730cf197742ef2_247x226.png', 3, 1,
        1, 0, '2024-10-22 14:21:05', '2024-10-22 14:21:05', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (313, 266, '金融经济',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/7dc0e39d472940d1bbe9c3518cc93fac_247x226.png', 3, 2,
        1, 0, '2024-10-22 14:21:22', '2024-10-22 14:21:22', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (314, 0, '玩具乐器',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/28/beec99f7f1a54f1fa50d5e07f1a6deac_230x230.png', 1, 15,
        0, 0, '2024-10-22 14:22:28', '2024-11-12 10:38:15', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (315, 314, '玩具',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/3779f4500c284a648104c6a9d764ea0b_247x226.png', 2, 2,
        1, 0, '2024-10-22 14:22:44', '2024-10-22 14:22:44', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (316, 314, '乐器',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/840ebc4a4f8d498381ea8f16a8a69c8b_247x226.png', 2, 1,
        1, 0, '2024-10-22 14:23:09', '2024-10-22 14:23:09', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (317, 315, '潮流盲盒',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/8daef9611a994d3f90a326bc82df020a_247x226.png', 3, 1,
        1, 0, '2024-10-22 14:23:23', '2024-10-22 14:23:23', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (318, 315, '毛绒玩具',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/37485b2342ca487a86ca8d7cbdb33bfc_247x226.png', 3, 2,
        1, 0, '2024-10-22 14:23:40', '2024-10-22 14:23:40', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (319, 315, '遥控/电动',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/1a070a3c986447aa863499fcde49193d_247x226.png', 3, 3,
        1, 0, '2024-10-22 14:23:53', '2024-10-22 14:23:53', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (320, 315, '积木拼装',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/76a54f0effff4d51a3ffab57db051965_247x226.png', 3, 4,
        1, 0, '2024-10-22 14:24:05', '2024-10-22 14:24:05', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (321, 315, '绘画/DIY',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/556c1c89d47147c0a078a9dab64d7c9f_247x226.png', 3, 5,
        1, 0, '2024-10-22 14:24:17', '2024-10-22 14:24:17', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (322, 315, '橡皮泥',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/42bb45daadb24db9a93125b4baeb298b_247x226.png', 3, 6,
        1, 0, '2024-10-22 14:24:29', '2024-10-22 14:24:29', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (323, 315, '益智玩具',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/eeb3712c4cae4521ad90b60f8ac55b95_247x226.png', 3, 7,
        1, 0, '2024-10-22 14:24:39', '2024-10-22 14:24:39', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (324, 316, '民族乐器',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/38f87baf8f554af898605f83336433e0_247x226.png', 3, 1,
        1, 0, '2024-10-22 14:26:04', '2024-10-22 14:26:04', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (325, 316, '西洋管弦',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/36f32df1c0a14835902781f8e6994e78_247x226.png', 3, 2,
        1, 0, '2024-10-22 14:26:16', '2024-10-22 14:26:16', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (326, 316, '打击乐器',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/37319587ef9a4d6484363db0d93c8d80_247x226.png', 3, 3,
        1, 0, '2024-10-22 14:26:29', '2024-10-22 14:26:29', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (327, 316, '尤克里里',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/4988586f50df4d34b4ce906848938c92_247x226.png', 3, 4,
        1, 0, '2024-10-22 14:26:40', '2024-10-22 14:26:40', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (328, 316, '吉他',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/6943d8ed787c44248c815d9daf682956_247x226.png', 3, 5,
        1, 0, '2024-10-22 14:26:51', '2024-10-22 14:26:51', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (329, 316, '电子琴',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/362e1da382f940fda46b2246f2b86405_247x226.png', 3, 6,
        1, 0, '2024-10-22 14:27:01', '2024-10-22 14:27:01', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (330, 316, '电钢琴',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/8a9bd23248fb4a59b7590290841c2405_247x226.png', 3, 7,
        1, 0, '2024-10-22 14:27:12', '2024-10-22 14:27:12', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (331, 316, '钢琴',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/41e058f1f6c144dfa11ddb0d472211b0_247x226.png', 3, 8,
        1, 0, '2024-10-22 14:27:22', '2024-10-22 14:27:22', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (332, 0, '茶酒饮品',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/28/973a7df566454dfa88a71829bbe0be6b_230x230.png', 1, 16,
        0, 0, '2024-10-22 14:27:47', '2024-11-12 10:38:17', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (333, 332, '饮料',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/9d7c7f5245d94cb897a5695ce952d352_247x226.png', 2, 1,
        1, 0, '2024-10-22 14:28:22', '2024-10-22 14:28:22', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (334, 332, '茗茶',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/f8db93dc7a20451085b3b174fea7f0c1_247x226.png', 2, 2,
        1, 0, '2024-10-22 14:28:42', '2024-10-22 14:28:42', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (335, 332, '中外名酒',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/e170b076b492420ab54e1e7eec8f910a_247x226.png', 2, 3,
        1, 0, '2024-10-22 14:29:01', '2024-10-22 14:29:01', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (336, 335, '果酒',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/b8fc018950974851bb083512ce6b1ea5_247x226.png', 3, 1,
        1, 0, '2024-10-22 14:30:36', '2024-10-22 14:30:36', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (337, 335, '啤酒',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/4d5bb856338a47928341469b2e842370_247x226.png', 3, 2,
        1, 0, '2024-10-22 14:31:40', '2024-10-22 14:31:40', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (338, 335, '洋酒',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/8f12305d81a74b47b56f66cac15ccfb3_247x226.png', 3, 3,
        1, 0, '2024-10-22 14:31:51', '2024-10-22 14:31:51', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (339, 335, '葡萄酒',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/6f062202a6d24a13a99442db956eaa2b_247x226.png', 3, 4,
        1, 0, '2024-10-22 14:32:01', '2024-10-22 14:32:01', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (340, 335, '白酒',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/be65fa1456974bac9481674c10f50cfb_247x226.png', 3, 5,
        1, 0, '2024-10-22 14:32:13', '2024-10-22 14:32:13', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (341, 334, '红茶',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/05a59fd999724f9991ceb3fdc2993827_247x226.png', 3, 1,
        1, 0, '2024-10-22 14:32:32', '2024-10-22 14:32:32', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (342, 334, '绿茶',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/4a65e95a72a54c82a418594c67d04c33_247x226.png', 3, 2,
        1, 0, '2024-10-22 14:32:43', '2024-10-22 14:32:43', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (343, 334, '龙井',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/5541faae918d49e987523768555ce82b_247x226.png', 3, 3,
        1, 0, '2024-10-22 14:32:54', '2024-10-22 14:32:54', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (344, 334, '普洱',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/c8237dc1b76349838b0305b9a3beaef2_247x226.png', 3, 4,
        1, 0, '2024-10-22 14:33:04', '2024-10-22 14:33:04', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (345, 334, '铁观音',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/d8c3a628f7a247878dabf68f41417191_247x226.png', 3, 5,
        1, 0, '2024-10-22 14:33:17', '2024-10-22 14:33:17', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (346, 333, '蜂蜜/蜂产品',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/ee55f256b19e46349d459af890e83948_247x226.png', 3, 1,
        1, 0, '2024-10-22 14:34:14', '2024-10-22 14:34:14', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (347, 333, '奶茶',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/0ad4adb1765b4dfdb6161647f87121ba_247x226.png', 3, 2,
        1, 0, '2024-10-22 14:34:29', '2024-10-22 14:34:29', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (348, 333, '咖啡',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/5ce47d8bf0534ce4b4b5187959fe6933_247x226.png', 3, 3,
        1, 0, '2024-10-22 14:34:39', '2024-10-22 14:34:39', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (349, 333, '饮用水',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/b0a71ae139fb48a88d80f48cbf62ee6f_247x226.png', 3, 4,
        1, 0, '2024-10-22 14:34:49', '2024-10-22 14:34:49', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (350, 333, '酸奶',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/4c25d1a53df54df2bb32716ea0618e94_247x226.png', 3, 5,
        1, 0, '2024-10-22 14:34:59', '2024-10-22 14:34:59', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (351, 333, '牛奶',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/d6b977222f284c398c6d7fa8abc94e16_247x226.png', 3, 6,
        1, 0, '2024-10-22 14:35:09', '2024-10-22 14:35:09', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (352, 333, '茶饮料',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/b130768c1e2b4ed7a264550630cbbedc_247x226.png', 3, 7,
        1, 0, '2024-10-22 14:35:19', '2024-10-22 14:35:19', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (353, 333, '果汁',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/d7e84a88788c4612acebd6bd82f2758f_247x226.png', 3, 8,
        1, 0, '2024-10-22 14:35:29', '2024-10-22 14:35:29', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (354, 333, '碳酸饮料',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/372c5d5c4fd843ed9854a3c331c4ee6a_247x226.png', 3, 9,
        1, 0, '2024-10-22 14:35:39', '2024-10-22 14:35:39', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (355, 25, '粮油调味',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/7584d23512d941b88839b7b738726d8b_247x226.png', 2, 1,
        1, 0, '2024-10-22 14:37:22', '2024-10-22 14:37:47', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (356, 25, '休闲食品',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/ab10b61c97224ac692dd69729c937b47_247x226.png', 2, 2,
        1, 0, '2024-10-22 14:37:33', '2024-10-22 14:37:51', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (357, 356, '进口食品',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/8c735bf74f194d568c5089ef52f70dd4_247x226.png', 3, 1,
        1, 0, '2024-10-22 14:38:05', '2024-10-22 14:38:05', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (358, 356, '冲调饮品',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/3aed3f1d1c624906babcc83b7fde5eeb_247x226.png', 3, 2,
        1, 0, '2024-10-22 14:38:18', '2024-10-22 14:38:18', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (359, 356, '糖果巧克力',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/33476a7557124b7d84746a1e8a833b14_247x226.png', 3, 3,
        1, 0, '2024-10-22 14:38:30', '2024-10-22 14:38:30', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (360, 356, '肉干辣条',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/1477a15f040a4e928641c26d08e0eac6_247x226.png', 3, 4,
        1, 0, '2024-10-22 14:38:43', '2024-10-22 14:38:43', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (361, 356, '坚果炒货',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/30b3878b3e6f4016a7441a3576e4672f_247x226.png', 3, 5,
        1, 0, '2024-10-22 14:38:54', '2024-10-22 14:38:54', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (362, 356, '蜜饯果干',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/2cf6d6768cf5464f80554ff255844dd8_247x226.png', 3, 6,
        1, 0, '2024-10-22 14:39:07', '2024-10-22 14:39:07', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (363, 356, '薯片/膨化',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/6a3fcf05ff9a4f95af2aa7956b6b65a7_247x226.png', 3, 7,
        1, 0, '2024-10-22 14:39:19', '2024-10-22 14:39:27', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (364, 356, '饼干及点心',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/4da4010a7a9f435daf79de04bb69f621_247x226.png', 3, 8,
        1, 0, '2024-10-22 14:39:39', '2024-10-22 14:39:39', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (365, 355, '方便食品',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/ffefe0bbc7ad49e1a336f4a063665f8f_247x226.png', 3, 1,
        1, 0, '2024-10-22 14:39:59', '2024-10-22 14:39:59', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (366, 355, '南北干货',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/9988f363e6044386baa305c5865fb0f7_247x226.png', 3, 2,
        1, 0, '2024-10-22 14:40:13', '2024-10-22 14:40:13', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (367, 355, '杂粮',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/f366a6f2dbeb433390baaadda16d7986_247x226.png', 3, 3,
        1, 0, '2024-10-22 14:40:29', '2024-10-22 14:40:29', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (368, 355, '调味品',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/4991e683e00e433aae8f15b495f03746_247x226.png', 3, 4,
        1, 0, '2024-10-22 14:40:43', '2024-10-22 14:40:43', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (369, 355, '米面',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/de697d1aea4342c7ab1cdafcba12dccf_247x226.png', 3, 5,
        1, 0, '2024-10-22 14:40:53', '2024-10-22 14:40:53', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (370, 355, '食用油',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/b6410cbfdcf44cb39885eefb916c538f_247x226.png', 3, 6,
        1, 0, '2024-10-22 14:41:04', '2024-10-22 14:41:04', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (371, 0, '日用百货',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/28/671bde7112634a92b23e6d8914e2393a_230x230.png', 1, 13,
        1, 0, '2024-10-22 14:41:45', '2024-10-28 11:52:11', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (372, 371, '家庭清洁',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/d7bde0eb0e814c6e9c54dd3706f355f3_247x226.png', 2, 1,
        1, 0, '2024-10-22 14:50:43', '2024-10-22 14:50:43', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (373, 371, '纸品清洗',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/9cd0ad740db4463d820e3045b5fbf45d_247x226.png', 2, 2,
        1, 0, '2024-10-22 14:50:57', '2024-10-22 14:50:57', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (374, 371, '收纳用品',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/30ce47ee29f94f9aac545e2fc7a85ad6_247x226.png', 2, 3,
        1, 0, '2024-10-22 14:51:10', '2024-10-22 14:51:10', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (375, 371, '生活日用',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/f0271a310c39491f86fa9e31c21e8dc5_247x226.png', 2, 4,
        1, 0, '2024-10-22 14:51:23', '2024-10-22 14:51:23', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (376, 375, '沐浴洗漱用品',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/9a692f233203454080db5c24c293e676_247x226.png', 3, 1,
        1, 0, '2024-10-22 14:51:43', '2024-10-22 14:51:43', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (377, 375, '衣物洗晒/护理用具',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/f67a856f888f4fa894410adadf8da05e_247x226.png', 3, 1,
        1, 0, '2024-10-22 14:51:59', '2024-10-22 14:51:59', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (378, 375, '浴室/置物用品',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/0d1f6118c2f94f498263365ab9f58adf_247x226.png', 3, 3,
        1, 0, '2024-10-22 14:52:13', '2024-10-22 14:52:13', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (379, 375, '日用杂货',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/bbc1761580d64a33a4bd305699bb3cd7_247x226.png', 3, 4,
        1, 0, '2024-10-22 14:52:23', '2024-10-22 14:52:23', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (380, 374, '防尘罩/膜',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/1beb0d215d7e422a9f49bc6663d0a688_247x226.png', 3, 1,
        1, 0, '2024-10-22 14:52:46', '2024-10-22 14:52:46', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (381, 374, '收纳箱',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/94a0dab30f924e75916328750b67d67f_247x226.png', 3, 2,
        1, 0, '2024-10-22 14:53:01', '2024-10-22 14:53:01', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (382, 374, '收纳袋',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/6931cb37bb4b49d89de7434f29254990_247x226.png', 3, 3,
        1, 0, '2024-10-22 14:53:11', '2024-10-22 14:53:11', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (383, 374, '收纳盒',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/3dc53d22eec540be9a4e0951672c5b8a_247x226.png', 3, 4,
        1, 0, '2024-10-22 14:53:23', '2024-10-22 14:53:23', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (384, 373, '护理除菌',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/f5576f821852435381de1598591d3659_247x226.png', 3, 1,
        1, 0, '2024-10-22 14:53:52', '2024-10-22 14:53:52', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (385, 373, '肥皂',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/ef8af6e30968467f9df7427fda97f9ec_247x226.png', 3, 2,
        1, 0, '2024-10-22 14:54:07', '2024-10-22 14:54:07', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (386, 373, '洗衣粉',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/5413615dd2e54e88a1d1b1c48c9f11a3_247x226.png', 3, 3,
        1, 0, '2024-10-22 14:54:18', '2024-10-22 14:54:18', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (387, 373, '洗衣液',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/5f604ac7acb541d7a279320b7a360f63_247x226.png', 3, 4,
        1, 0, '2024-10-22 14:54:27', '2024-10-22 14:54:27', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (388, 373, '洗衣凝珠',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/5e577c6bdcef4f5fa6bad1cb1551ad02_247x226.png', 3, 5,
        1, 0, '2024-10-22 14:54:36', '2024-10-22 14:54:36', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (389, 373, '厨房用纸',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/c515f11a1d4d479e97a424bf506e01b3_247x226.png', 3, 6,
        1, 0, '2024-10-22 14:54:46', '2024-10-22 14:54:46', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (390, 373, '湿巾',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/c7f82cd95e144b9ea285f2ef55b66885_247x226.png', 3, 7,
        1, 0, '2024-10-22 14:54:56', '2024-10-22 14:54:56', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (391, 373, '卷纸',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/db862bfd67844c2bbc0df2189c7f10d3_247x226.png', 3, 8,
        1, 0, '2024-10-22 14:55:08', '2024-10-22 14:55:08', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (392, 373, '抽纸',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/f940cf41739d4c0994ea4246aba0f00f_247x226.png', 3, 9,
        1, 0, '2024-10-22 14:55:18', '2024-10-22 14:55:18', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (393, 372, '驱蚊驱虫',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/71a50679738d4f0ab363dbd3b23f3bd0_247x226.png', 3, 1,
        1, 0, '2024-10-22 14:55:38', '2024-10-22 14:55:38', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (394, 372, '拖把/扫把',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/194b8e21cdf6468da8e726fbf552b77f_247x226.png', 3, 2,
        1, 0, '2024-10-22 14:55:58', '2024-10-22 14:55:58', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (395, 372, '垃圾桶',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/7d3b17f97e1140c3ad9cf564b443da12_247x226.png', 3, 3,
        1, 0, '2024-10-22 14:56:12', '2024-10-22 14:56:12', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (396, 372, '垃圾袋',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/097e096e13c94931ac650ddaf31e824e_247x226.png', 3, 4,
        1, 0, '2024-10-22 14:56:24', '2024-10-22 14:56:24', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (397, 372, '地板清洁剂',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/be53e1306794456292fa56b41dd158b7_247x226.png', 3, 5,
        1, 0, '2024-10-22 14:56:36', '2024-10-22 14:56:36', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (398, 372, '消毒液',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/1734bcb720404ea8ba96ab5a4e521f28_247x226.png', 3, 6,
        1, 0, '2024-10-22 14:56:48', '2024-10-22 14:56:48', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (399, 372, '洁厕剂',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/769df337c9344d48a688aac6012939af_247x226.png', 3, 7,
        1, 0, '2024-10-22 14:56:58', '2024-10-22 14:56:58', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (400, 372, '油污净',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/95ccc4b0a5734b658c6f8954a53dc1ed_247x226.png', 3, 8,
        1, 0, '2024-10-22 14:57:08', '2024-10-22 14:57:08', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (401, 372, '洗洁精',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/36b9b7b1087c4e25b4240e1ffa98b09b_247x226.png', 3, 9,
        1, 0, '2024-10-22 14:57:18', '2024-10-22 14:57:18', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (402, 91, '女装套装',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/6809a872f526467394687fac23e97792_247x226.png', 2, 1,
        1, 0, '2024-10-22 14:58:10', '2024-10-22 14:58:10', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (403, 91, '女装下装',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/26b3edf13ebb46b8872221ae7d005461_247x226.png', 2, 2,
        1, 0, '2024-10-22 14:58:37', '2024-10-22 14:58:37', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (404, 91, '女装上装',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/25b763a90a2a4876afa1166d849f4982_247x226.png', 2, 3,
        1, 0, '2024-10-22 14:58:51', '2024-10-22 14:58:51', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (405, 402, '女装睡衣',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/3748f23aa7ba4ad98532da24111a2e04_247x226.png', 3, 1,
        1, 0, '2024-10-22 14:59:13', '2024-10-22 14:59:13', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (406, 402, '裙子',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/fce4d4900c60464d9d57339544391fc3_247x226.png', 3, 2,
        1, 0, '2024-10-22 14:59:44', '2024-10-22 14:59:44', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (407, 403, '女装西裤',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/830dbd6c9fc84b12adc6d9d4f747b3e6_247x226.png', 3, 1,
        1, 0, '2024-10-22 15:00:04', '2024-10-22 15:00:04', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (408, 403, '女装牛仔裤',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/b0adbde945f7425083ac804c96d972cd_247x226.png', 3, 2,
        1, 0, '2024-10-22 15:00:22', '2024-10-22 15:00:22', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (409, 403, '女装休闲裤',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/96d2ea7d600544d59a4c46b230e4d88f_247x226.png', 3, 3,
        1, 0, '2024-10-22 15:00:48', '2024-10-22 15:00:48', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (410, 404, '女装衬衫',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/75b818aacb574727879288da954f6b69_247x226.png', 3, 1,
        1, 0, '2024-10-22 15:01:14', '2024-10-22 15:01:14', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (411, 404, '女装T恤',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/163f67591ea44b6aa3575eb75365115d_247x226.png', 3, 2,
        1, 0, '2024-10-22 15:01:32', '2024-10-22 15:01:32', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (412, 404, '女装卫衣',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/e3296f4dffa94e958ddd2fc4a0310b3f_247x226.png', 3, 3,
        1, 0, '2024-10-22 15:01:45', '2024-10-22 15:01:45', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (413, 404, '女装外套',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/22/8598359eeb3b4fa29027924f2d95aa52_247x226.png', 3, 4,
        1, 0, '2024-10-22 15:02:00', '2024-10-22 15:02:00', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (414, 0, '其他商品',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/28/ddcd3fe6fce74079a5fd440b3f7d5555_230x230.png', 1, 26,
        1, 0, '2024-10-28 15:52:16', '2024-10-28 15:55:18', 'sohuglobal');
INSERT INTO `sohu_product_category_pc_copy1` (`id`, `pid`, `name`, `icon`, `level`, `sort`, `is_show`, `is_del`,
                                              `create_time`, `update_time`, `sys_source`)
VALUES (415, 414, '兑换卡（码）',
        'https://sohugloba.oss-cn-beijing.aliyuncs.com/2024/10/28/03444a918081441b98928f0a7cd1bb70_230x230.png', 2, 1,
        1, 0, '2024-10-28 15:53:10', '2024-10-28 15:58:03', 'sohuglobal');
COMMIT;

SET
FOREIGN_KEY_CHECKS = 1;
