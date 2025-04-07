# 综合 API 文档

## 1. 登录获取 Token

### 请求方式

- **POST**

### URL

```
https://api.sohuglobal.com/auth/v2/login
```

### 请求参数

```json
{
    "phone": "admin",
    "code": "",
    "loginType": "PASSWORD",
    "password": "lgjWOlXw7c95y1YS9UJ8mw==",
    "userName": "admin"
}
```

### 返回示例

```json
{
    "code": 200,
    "msg": "操作成功",
    "data": {
        "userId": 1,
        "accessToken": "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJsb2dpblR5cGUiOiJsb2dpbiIsImxvZ2luSWQiOiJzeXNfdXNlcjoxIiwicm5TdHIiOiJuZmpHVjZVdDd3SXlXQXF3dFlKeXk2eVdsSHFBNzBnRyIsInVzZXJJZCI6MX0.zn4EgVldxzOV-KLRq0zLEZxRl4aJcf6UMR9EdPEHhVs",
        "expiresTime": 604800,
        "tag": null,
        "flag": true,
        "uuid": null,
        "havePhone": null
    }
}
```

### 注意事项

- `accessToken` 需要拼接 `Bearer `（注意空格）后在后续接口的 `Authorization` Header 中使用。

---

## 2. 审核列表接口

### 请求方式

- **GET**

### URL

```
https://api.sohuglobal.com/admin/audit/list?pageSize=10&pageNum=1&state=OnShelf&busyType=Article&roleKey=admin
```

### 参数说明

| 参数名      | 类型     | 必填 | 描述                                                           |
|----------|--------|----|--------------------------------------------------------------|
| pageSize | int    | 是  | 每页条数                                                         |
| pageNum  | int    | 是  | 当前页码                                                         |
| state    | string | 是  | 状态：已通过（OnShelf）、审核中（WaitApprove）、已拒绝（Refuse）、强制下架（CompelOff） |
| busyType | string | 是  | 类型：图文（Article）、视频（Video）                                     |

---

## 3. 商品列表接口

### 请求方式

- **GET**

### URL

```
https://api.sohuglobal.com/shop-goods/product/pc/list?pageSize=10&pageNum=1&type=2
```

### 参数说明

| 参数名      | 类型  | 必填 | 描述                                 |
|----------|-----|----|------------------------------------|
| pageSize | int | 是  | 每页条数                               |
| pageNum  | int | 是  | 当前页码                               |
| type     | int | 是  | 商品状态：出售中（1）、仓库中（2）、待审核（6）、审核未通过（7） |

---

## 4. 视频/图文列表接口

### 请求方式

- **GET**

### URL

```
https://api.sohuglobal.com/admin/playlet/list?pageSize=10&pageNum=1&state=OnShelf
```

### 参数说明

| 参数名      | 类型     | 必填 | 描述                                                           |
|----------|--------|----|--------------------------------------------------------------|
| pageSize | int    | 是  | 每页条数                                                         |
| pageNum  | int    | 是  | 当前页码                                                         |
| state    | string | 是  | 状态：已通过（OnShelf）、审核中（WaitApprove）、已拒绝（Refuse）、强制下架（CompelOff） |
| busyCode | int    | 否  | 业务编码（可从其他接口返回值中复用）                                           |
| busyType | string | 否  | 业务类型：图文（Article）、视频（Video）                                   |

---

## 5. 评论列表接口

### 请求方式

- **GET**

### URL

```
https://api.sohuglobal.com/app/api/common/comment/list?busyCode=7549&busyType=Video&pageNum=1&pageSize=10
```

### 参数说明

| 参数名      | 类型     | 必填 | 描述                         |
|----------|--------|----|----------------------------|
| busyCode | int    | 是  | 业务编码（可从其他接口返回值中复用）         |
| busyType | string | 是  | 业务类型：图文（Article）、视频（Video） |
| pageSize | int    | 是  | 每页条数                       |
| pageNum  | int    | 是  | 当前页码                       |

---

## 6. 简介详情接口

### 请求方式

- **GET**

### URL

```
https://api.sohuglobal.com/app/api/content/video/7549
```

### 参数说明

| 参数名      | 类型  | 必填 | 描述                 |
|----------|-----|----|--------------------|
| busyCode | int | 是  | 业务编码（可从其他接口返回值中复用） |

### 返回字段说明

| 字段名          | 类型  | 描述  |
|--------------|-----|-----|
| viewCount    | int | 阅读数 |
| commentCount | int | 评论数 |
| praiseCount  | int | 点赞数 |
| collectCount | int | 收藏数 |
| forwardCount | int | 转发数 |

---

## 7. 查询用户行为记录列表

### 请求方式

- **GET**

### URL

```
/open/user/behavior/list
```

### 请求参数

| 参数名称         | 参数说明 | 是否必须  | 数据类型    | 备注                                                                                                              |
|:-------------|:-----|:------|:--------|:----------------------------------------------------------------------------------------------------------------|
| userId       | 用户id | true  | Long    | 用户id                                                                                                            |
| operaSource  | 操作来源 | false | Integer | 0.其他 1.web端  2.安卓  3.IOS 4.小程序                                                                                  |
| businessType | 业务类型 | false | String  | Article 图文  Video 视频  Question 问答 shortPlay 短剧  busyOrder  商单 Novel 小说  Game 游戏  Literature 诗歌散文 User 人员  Ad 广告 | 
| operaType    | 操作类型 | false | Integer | 1、查看列表 2、查看详情 3、点赞 4、评论 5、收藏 6、关注 7、打赏 8、新建 9、编辑  10、删除 11、下架  12、转发                                            |
| startTime    | 开始时间 | false | Date    | 开始时间,会查询大于该时间的数据                                                                                                |

### 响应参数

| 参数名称         | 参数说明 | 类型      | 备注                                                                                                              | 
|:-------------|:-----|:--------|:----------------------------------------------------------------------------------------------------------------|
| id           | 记录id | Long    | 记录id                                                                                                            |
| userId       | 用户id | Long    | 用户id                                                                                                            |
| operaSource  | 操作来源 | Integer | 0.其他  1.web端  2.安卓  3.IOS  4.小程序                                                                                |
| businessType | 业务类型 | String  | Article 图文  Video 视频  Question 问答 shortPlay 短剧  busyOrder  商单 Novel 小说  Game 游戏  Literature 诗歌散文 User 人员  Ad 广告 |
| operaType    | 操作类型 | Integer | 1、查看列表 2、查看详情 3、点赞 4、评论 5、收藏 6、关注 7、打赏 8、新建 9、编辑  10、删除 11、下架  12、转发                                            |
| createTime   | 操作时间 | Date    | 操作时间                                                                                                            |

### 响应示例

```javascript
{
    "code"
:
    200,
        "msg"
:
    "查询成功",
        "data"
:
    [{
        "id": 1,
        "userId": 3,
        "operaSource": 2,
        "businessType": "Article",
        "operaType": 3,
        "createTime": "2025-02-21 13:15:59",
    }]
}
```

---

## 8. 获取用户行为记录详细信息

### 请求方式

- **GET**

### URL

```
/open/user/behavior/{id}
```

### 请求参数

| 参数名称 | 参数说明 | 是否必须 | 数据类型 | 备注   |
|:-----|:-----|:-----|:-----|:-----|
| id   | 记录id | true | Long | 记录id |

### 响应参数

| 参数名称         | 参数说明      | 类型      | 备注                                                                                                              | 
|:-------------|:----------|:--------|:----------------------------------------------------------------------------------------------------------------|
| id           | 记录id      | Long    | 记录id                                                                                                            |
| userId       | 用户id      | Long    | 用户id                                                                                                            |
| operaSource  | 操作来源      | Integer | 0.其他  1.web端  2.安卓  3.IOS  4.小程序                                                                                |
| businessType | 业务类型      | String  | Article 图文  Video 视频  Question 问答 shortPlay 短剧  busyOrder  商单 Novel 小说  Game 游戏  Literature 诗歌散文 User 人员  Ad 广告 |
| operaType    | 操作类型      | Integer | 1、查看列表 2、查看详情 3、点赞 4、评论 5、收藏 6、关注 7、打赏 8、新建 9、编辑  10、删除 11、下架  12、转发                                            |
| createTime   | 操作时间      | Date    | 操作时间                                                                                                            |
| operIp       | 请求的ip     | String  | 请求的ip                                                                                                           |
| operResult   | 处理结果json串 | String  | 例如：点赞某个视频   则该json 为视频信息的主要数据。 数据主要包括  业务主键id、标题、创作人id、分类id等基础数据。列表，下架，评论，收藏，转发，新建，编辑和删除查明细，不会有结果返回。 **因此有结果返回的为点赞、关注、打赏、下架、转发。**             |

### operResult 内部对象

| 参数名称         | 参数说明       | 类型     | 备注                                       | 
|:-------------|:-----------|:-------|:-----------------------------------------|
| businessId   | 业务主键id     | Long   | 业务主键id                                   |
| title        | 业务内容标题     | String | 业务内容标题                                   |
| userId       | 用户id/创作人id | Long   | 该字段在特定情况不会存在 例如：广告没有创建人id                |
| categoryId   | 分类ID       | Long   | 该字段在特定情况不会存在,例如：广告没有分类, 关注某个人的时候也不存在分类信息 |
| categoryName | 分类名称       | String | 分类id对应的分类名称 （商单是行业分类，小说是小说分类）            |

### 响应示例

```javascript
{
    "code"
:
    200,
        "msg"
:
    "查询成功",
        "data"
:
    [{
        "id": 1,
        "userId": 3,
        "operaSource": 2,
        "businessType": "Article",
        "operaType": 3,
        "createTime": "2025-02-21 13:15:59",
        "operIp": "192.168.0.158",
        "operResult": "{\"businessId\":1268,\"title\":\"这是个测试图文\",\"userId\":70,\"categoryId\":251,\"categoryName\":\"媒体娱乐\"}"
    }]
}
```

---

## 注意事项

1. 所有接口需要在请求头中携带 `Authorization`，格式为：
   ```
   Authorization: Bearer <accessToken>
   ```
2. `busyCode` 和 `busyType` 可以从其他接口返回值中复用。
3. 接口分类和状态值需根据实际需求选择对应的参数。
