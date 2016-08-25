# idmapping

Mapping流程
1. Build User Linkage
根据数据中的标识（device id， cookie， email等）查找DMP的userid
如果数据中有多个标识，每个标识对应的userid都必须找出来
如果所有id都未找到对应的userid，则认为是一个新用户，新建一个userid
如果所有id对应同一个userid，则以找到的userid作为用户id
如果找到的userid不止一个，则采用第一个找到的userid作为用户id
2. Build Mapping Relation
更新所有id-user映射表中的映射关系到找到的用户id
3. Append Event
如果数据为事件类型，在事件表中给对应的用户增加一条事件记录
4. Update User Profile 
将用户的Profile信息中指定的属性更新为数据中对应的属性值
5. Update User Aliases
对于存在多个userid的情况，在用户表的alias列簇中给每个userid增加一列，列名为userid，值为空
6. Update User Devices
在用户表的devices列簇中给每个id增加一列，列名为source\001id，值为id类型（imei，androidid，idfa或其他）
7. Update Event Counters
在用户表中更新用户对应的默认和指定的计数器
默认计数器：
eventname@sum 事件发生次数
eventname@last 事件最后一次发生时间
指定计数器，可通过配置项配置
eventname@price 用户购买物件价格总和
8. 其他
mapping时历史id -> user关系不做更新
mapping时历史event数据不做更新
