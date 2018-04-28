import random

from pyspark import SparkContext, SparkConf
from pyspark.storagelevel import StorageLevel

from record import Record

# Aggregate
def seqOp(r, v):
    return r.addAmt(v)


# Aggregate
def combOp(r1, r2):
    return r1 + r2













# 1 Map
rdd1 = sc.parallelize(range(1, 6))
rdd2 = rdd1.map(lambda v: v + 1)
print(rdd2.collect())

# 출력 - 2,3,4,5,6

        
# 2 flatMap
# 하나의 입력값에 여러 개의 출력값이 대응될때 편리
rdd1 = sc.parallelize(["apple,orange", "grape,apple,mango", "blueberry,tomato,orange"])
rdd2 = rdd1.flatMap(lambda s: s.split(","))
print(rdd2.collect())

# 출력 - apple,orange,grape,apple,mango,blueberry,tomato,orange


# 3 mapPartitions
# Map은 요소별로 처리하는 반면 여기선 Partition단위로 처리한다.
rdd1 = sc.parallelize(range(1, 11), 3)
rdd2 = rdd1.mapPartitions(increase)
print(rdd2.collect())
        
    def increase(numbers):
        print("DB 연결 !!!")
        return (i + 1 for i in numbers)

# 출력 - DB 연결 !!! DB 연결 !!! DB 연결 !!! 2,3,4,5,6,7,8,9,10,11


# 4 mapPartitionsWithIndex
# 전달되는 함수를 호출할 때 해당 파티션의 Index정보도 함께 전달해준다.
rdd1 = sc.parallelize(range(1, 11), 3)
rdd2 = rdd1.mapPartitionsWithIndex(increaseWithIndex)
        print(rdd2.collect())
    def increaseWithIndex(idx, numbers):
        for i in numbers:
            if (idx == 1):
                yield i
# 출력 - 5,6,7 (때에 따라 다름) idx=0이 첫번째 partition


# 5 mapValues
# pairRDD인경우만 사용 가능(키,값의 쌍) 값에만 메서드 적용
rdd1 = sc.parallelize(["a", "b", "c"])
rdd2 = rdd1.map(lambda v: (v, 1))
rdd3 = rdd2.mapValues(lambda i: i + 1)
print(rdd3.collect())
#출력 - [('a',2), ('b',2), ('c',2)]


# 6 flatMapValues
# pairRDD인경우만 flatMap메서드를 값에만 적용
rdd1 = sc.parallelize([(1, "a,b"), (2, "a,c"), (1, "d,e")])
rdd2 = rdd1.flatMapValues(lambda s: s.split(","))
print(rdd2.collect())
#출력 - [(1,'a'),(1,'b'),(2,'a'),(2,'c'),(1,'d'),(1,'e')


# 7 zip
# 파티션의 개수와 요소의 수가 같아야만 한다.
rdd1 = sc.parallelize(["a", "b", "c"])
rdd2 = sc.parallelize([1, 2, 3])
result = rdd1.zip(rdd2)
print(result.collect())
# 출력 - [('a',1),('b',2),('c',3)]
# 그러나 파티션의 개수를 지정안하면 몇개만들어지는지 모르는데..


# 8 zipPartitions() - 파이썬 사용 불가


# 9 groupBy
# RDD의 요소들을 특정기준에 따라 분류해서, 키 와 sequence로 구성 해준다.
rdd1 = sc.parallelize(range(1, 11))
rdd2 = rdd1.groupBy(lambda v: "even" if v % 2 == 0 else "odd")
for x in rdd2.collect():
print(x[0], list(x[1]))
# 출력 ('even', [2, 4, 6, 8, 10])
#      ('odd', [1, 3, 5, 7 ,9])
# Question :
# rdd2 : [("even",[2,4,6,8,10]),("odd",[1,3,5,7,9])] 일텐데
# 이렇게 rdd2를 바로 만들면 print(rdd2.collect()) 실행 가능
# list 의 기능?


# 10 groupByKey
# pairRDD 일때 사용. 같은 키를 가진 pair들의 값을 sequence로 묶어준다.
rdd1 = sc.parallelize(["a", "b", "c", "b", "c"]).map(lambda v: (v, 1))
rdd2 = rdd1.groupByKey()
for x in rdd2.collect():
print(x[0], list(x[1]))
#출력 - ('a', [1]) ('b', [1,1]) ('c', [1,1])


# 11 cogroup
# 2개의 RDD에서 같은 키를 같는 값들을 합쳐주는 것.
# cogroup 전에 각각 groupByKey()를 실행한다고 생각.
# 그 실행 결과로 나온 sequence 를 합쳐서 tuple로 만드는 것
rdd1 = sc.parallelize([("k1", "v1"), ("k2", "v2"), ("k1", "v3")])
rdd2 = sc.parallelize([("k1", "v4")])
result = rdd1.cogroup(rdd2)
for x in result.collect():
print(x[0], list(x[1][0]), list(x[1][1])) 
#출력 -  ('k2', [v2], [])
#        ('k1', ['v1','v3'], ['v4'])
# x[1] 은 tuple, x[1][0]은 sequence 이를 list로 변환하는..느낌?
#이때의 result : [("k1", (["v1","v3"], ["v4"])), ("k2", (["v2"], []))] 로 존재
# 이렇게 바로 만들면 또 바로 print(result.collect()) 되는데...


# 12 distinct
# 그냥 중복 제거
rdd = sc.parallelize([1, 2, 3, 1, 2, 3, 1, 2, 3])
result = rdd.distinct()
print(result.collect())
#출력 - 1,2,3


# 13 cartesian
# cartesian곱 수행.. 그냥 모든 순서쌍을 구하는듯?
rdd1 = sc.parallelize([1, 2, 3])
rdd2 = sc.parallelize(["a", "b", "c"])
result = rdd1.cartesian(rdd2)
print(result.collect())
#출력 [(1,'a'),(1,'b'),(1,'c'),(2,'a'),(2,'b'),(2,'c'),(3,'a'),(3,'b'),(3,'c')]
# rdd1 이 Key, rdd2 가 value 가 된다.


# 14 subtract
# 차집함 
rdd1 = sc.parallelize(["a", "b", "c", "d", "e"])
rdd2 = sc.parallelize(["d", "e"])
result = rdd1.subtract(rdd2)
print(result.collect())
# 출력 - ['a', 'b', 'c']


#15 union
# 합집합
rdd1 = sc.parallelize(["a", "b", "c"])
rdd2 = sc.parallelize(["d", "e", "f"])
result = rdd1.union(rdd2)
print(result.collect())
# 출력 - ['a', 'b', 'c', 'd', 'e', 'f']


#16 intersection
# 교집합 + distinct() 수행 ( 중복제거 ) // 위의 차집합이나 합집합은 중복 그대로 나타남
rdd1 = sc.parallelize(["a", "a", "b", "c"])
rdd2 = sc.parallelize(["a", "a", "c", "c"])
result = rdd1.intersection(rdd2)
print(result.collect())
# 출력 - ['a','c']


#17 join
# 같은 키를 가지고 있는것들 끼리 합침.
rdd1 = sc.parallelize(["a", "b", "c", "d", "e"]).map(lambda v: (v, 1))
rdd2 = sc.parallelize(["b", "c"]).map(lambda v: (v, 2))
result = rdd1.join(rdd2)
print(result.collect())
# 출력 - [('b',(1,2)), ('c', (1,2))]

# cogroup 과의 차이점 ? join은 공통으로 존재하는 키값만 남긴다.
# join은 바로 print(result.collect()) 가능. (아마 바로 tuple 형태로 만들어줘서?)
# join은 cogroup처럼 groupByKey 작업을 생략.. 즉 하나의 RDD에 같은 키값이
# 존재한다해도 상관하지 않음. (tuple 내부에 sequence 존재하지 않는다)

# ex
# rdd1=sc.parallelize([1,2,1,2,3]).map(lambda v:(v,1))
# rdd2=sc.parallelize([2,3,2,4]).map(lambda v:(v,2))

# 출력 - [((2,(1,2)), (2,(1,2)),(2,(1,2)),(2,(1,2)),(3,(1,2))]





# 18 leftOuterJoin, rightOuterJoin
# left는 왼쪽의 키값을 기준으로 right 는 오른쪽을 기준으
rdd1 = sc.parallelize(["a", "b", "c"]).map(lambda v: (v, 1))
rdd2 = sc.parallelize(["b", "c"]).map(lambda v: (v, 2))
result1 = rdd1.leftOuterJoin(rdd2)
result2 = rdd1.rightOuterJoin(rdd2)
print("Left: %s" % result1.collect())
print("Right: %s" % result2.collect())
# 출력 - Left : [(a,(1,None)), (b,(1,2)), (c,(1,2))]
#        right : [(b,(1,2)), (c,(1,2))]
# 값은 rdd1. 이므로 rdd1 먼저 표기 된다.


# 19 subtractByKey
# 같은키 존재하는것 빼준다.
rdd1 = sc.parallelize(["a", "b"]).map(lambda v: (v, 1))
rdd2 = sc.parallelize(["b"]).map(lambda v: (v, 1))
result = rdd1.subtractByKey(rdd2)
print(result.collect())
# 출력 - [('a',1)]


# 20 reduceByKey
# 인자로 두개의 값을 연산하여 하나로 만드는 함수를 전달한다.
# 이때의 연산은 교환/결합법칙 보장되어야함 (파티션에 분산되서 순서가 다를수도 있음)
rdd = sc.parallelize(["a", "b", "b"]).map(lambda v: (v, 1))
result = rdd.reduceByKey(lambda v1, v2: v1 + v2)
print(result.collect())
# 출력 - [('a',1),('b',2)]



# 21 foldByKey
# 연산의 초기값을 결정할수있는데.. 이걸 어따쓸지 모르겠다.
rdd = sc.parallelize(["a", "b", "b"]).map(lambda v: (v, 1))
result = rdd.foldByKey(0, lambda v1, v2: v1 + v2)
print(result.collect())
# 출력 - [('a',1),('b',2)]




# 22 combineByKey
rdd = sc.parallelize([("Math", 100), ("Eng", 80), ("Math", 50), ("Eng", 70), ("Eng", 90)])
result = rdd.combineByKey(lambda v: createCombiner(v), lambda c, v: mergeValue(c, v),
                                  lambda c1, c2: mergeCombiners(c1, c2))
print('Math', result.collectAsMap()['Math'], 'Eng', result.collectAsMap()['Eng'])
# 왜 c: 이 아닐까요..
# __repr__ 은 왜..
# collectAsMap()['Math'] ??



# 23 aggregateByKey
# combineByKey 의 특수한 경우.. 
rdd = sc.parallelize([("Math", 100), ("Eng", 80), ("Math", 50), ("Eng", 70), ("Eng", 90)])
result = rdd.aggregateByKey(Record(0, 0), lambda c, v: mergeValue(c, v), lambda c1, c2: mergeCombiners(c1, c2))
print('Math', result.collectAsMap()['Math'], 'Eng', result.collectAsMap()['Eng'])


# 24 pipe
# 외부 프로세스 활용 가능
rdd = sc.parallelize(["1,2,3", "4,5,6", "7,8,9"])
result = rdd.pipe("cut -f 1,3 -d ,")
print(result.collect())
#출력 - [u'1,3', u'4,6', u'7,9']
# -f : 뽑아낼 필드 번호
# -d : 필드 구분자
# u가 왜나오는지 모르겠다.


# 25 coalesce / repartition
# partitions 개수 조절 메서드
# repartition 은 늘리고 줄이고 둘다가능인데, 셔플기반이라 느림
# coalesce 줄이는것만 가능이지만 셔플사용 안할수도 있어서 빠름
rdd1 = sc.parallelize(list(range(1, 11)), 10)
rdd2 = rdd1.coalesce(5)
rdd3 = rdd2.repartition(10)
print("partition size: %d" % rdd1.getNumPartitions())
print("partition size: %d" % rdd2.getNumPartitions())
print("partition size: %d" % rdd3.getNumPartitions())



# 26 repartitionAndSortWithinPartitions
# RDD를 구성하는 데이터를 키값에 따라 여러개의 파티션으로 분류, 정렬 수행
data = [random.randrange(1, 100) for i in range(0, 10)]
rdd1 = sc.parallelize(data).map(lambda v: (v, "-"))
rdd2 = rdd1.repartitionAndSortWithinPartitions(3, lambda x: x)
rdd2.foreachPartition(lambda values: print(list(values)))
# random. 이 정의가 안됬다고 나옴..


# 27 partitionBy
# RDD의 요소가 키,값 쌍일때 적용할수 있다..차이 말곤 repartition과 뭐가 다른지 모르겠다.
rdd1 = sc.parallelize([("apple", 1), ("mouse", 1), ("monitor", 1)], 5)
rdd2 = rdd1.partitionBy(3)
print("rdd1: %d, rdd2: %d" % (rdd1.getNumPartitions(), rdd2.getNumPartitions()))



#28 filter
rdd1 = sc.parallelize(range(1, 6))
rdd2 = rdd1.filter(lambda i: i > 2)
print(rdd2.collect())
# 출력 - [3,4,5]



#29 sortByKey
# 키값 기준 정렬
rdd = sc.parallelize([("q", 1), ("z", 1), ("a", 1)])
result = rdd.sortByKey()
print(result.collect())
# 출력 - [("a", 1), ("q", 1), ("z", 1)]


#30 keys / values
# 키, 값 추출
rdd = sc.parallelize([("k1", "v1"), ("k2", "v2"), ("k3", "v3")])
print(rdd.keys().collect())
print(rdd.values().collect())
# 출력 - ['k1', 'k2', 'k3'], ['v1', 'v2', 'v3]


#31 sample
# 비복원 추출일때 각요소가 포함될 확률 0~1
# 복원 추출일때 평균값 0~
rdd = sc.parallelize(range(1, 101))
result1 = rdd.sample(False, 0.5, 100) # 평균적으로 50개가 포함
result2 = rdd.sample(True, 1.5, 100) # 평균적으로 150 개가 포함.
print(result1.take(5))
print(result2.take(5))
  
sc.stop()
