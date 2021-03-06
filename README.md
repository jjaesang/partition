
# 프로그램 설명

정의
- 파일에 저장된 단어를 복수의 파일로 분리하는 프로그램

---

# 개발 환경 및 실행 방법

- macOS Mojave 10.14.4
- IntelliJ 2018.3.6
- JDK 1.8.0_201

```bash
$ git clone https://github.com/jjaesang/partition.git
$ cd partition
$ mkdir output # output을 저장할 directory 생성
$ ./mvnw clean package -DskipTests=true -Dbuild.name=partition-job shade:shade
$ java -jar target/partition-job.jar input/words.txt output 10
$ echo " partitioning file is finished.. thanks "
```

---

# 프로그램 설명 및 주요 고려사항
해당 프로그램은 Producer-Consumer 패턴을 응용하여, 단어를 복수의 파일로 분리하는 프로그램을 설계 

## 1. Producer 
파일을 읽어온 단어를 **유효한지 검사** 후, 단어에 대한 **키**를 생성하여 **키/값 형태의 메세지**로 매핑하여 Broker에게 전달하는 프로세스

### 키 생성 방법
- 키값은 알파벳으로 시작하는 경우, **소문자화하여 첫번째 글자**로 키 설정
    - e.g. Aac -> a / aac -> a / Zzz -> z  ..
- 숫자로 시작하는 경우, **number**로 변경
    - e.g. 1-point -> number / 2abe -> number


### 키를 다음과 같은 방법으로 선정한 이유 
1. 동일한 단어는 같 파티션에 할당해야함 
    -  동일한 단어는 첫번째 글자도 같기 때문에, 키값으로 파티셔닝시 동일한 단어는 같은 파티션에 할당되는 것을 보장함 

2. 단어가 알파벳으로 시작하면 첫 알파벳에 해당되는 파일에 써야하며, 숫자로 시작하는 단어는 number.txt에 할당해야함
    - 키값을 그대로 재사용하여 **파일명으로 활용**하기 위함 


## 2. Broker
Producer로 부터 받은 메세지를 파티셔닝하여 파티션을 관리하며, Consumer 요청에 따라 메세지 전달
> 각 파티션 당, 메세지를 보관하는 큐와 할당된 메세지의 키의 메타정보 관리

### 파티셔닝
키/값 파티셔닝의 방법론 중, 해당 데이터 특성에 맞추어 **AsciiCode 기반 파티셔너** 생성

키값에 대한 hashcode 기준 해시 파티셔너 사용시, 다음과 같은 **문제**가 발생할 수 있음

1. 키값이 입력으로 주어진 파티션 개수(N)만큼 균등하게 할당되지 않을 수 있음
2. hashcode는 음수가 될 수 있기 때문에, 음수 처리하지 않는다면 N + 1개의 파티션으로 분할될 수 있음
3. hashcode을 양수로 변환 작업하더라도, 한 개의 파티션에 2개의 키값이 할당될 수 있음
> - 27개의 파티션으로 나눌 시, 한개의 파티션에 2개의 키값이 들어갈 수 있음 
> - e.g. Partition#4 -> [number , i] 

다음과 같은 이유로, hashcode 대신 AsciiCode 기반으로 파티셔너 설계

키값의 범위와 파티션의 최대 개수를 알고 있기 때문에, AsciiCode값 기반의 연속된 숫자로 정의
>  - 최대 27개로 파티션을 나누더라도, 하나의 파티션에서는 한개의 키만 할당을 보장하기 위함 
>  - number에 대한 키는 AsciiCode z의 다음번호인 '{'로 변환하여 파티셔닝을 수행 


### Consumer Thread에게 파티션 할당
 - Broker는 Consumer의 요청이 들어올 시, 파티션과 Consumer를 1:1로 할당
     > 할당 원칙은 FIFO


## 3. Consumer
Broker로부터 Consuming할 파티션을 할당받아, 해당 파티션에 대한 메세지를 읽어와 파일에 저장

### 키값에 해당되는 파일에 단어 저장
 - Broker에게 할당받은 파티션 정보 기반으로 해당 키마다 쓰여질 파일 쓰는 객체 생성
    -  파티션에서 하나씩 읽어와, 키 값에 해당되는 writer객체에게 전달하여 데이터 저장
    -  각 writer는 저장하기 전, 자신만의 중복검사하는 셋을 관리하여 저장 여부를 판단
    -  파티션의 쌓여있는 데이터를 다 처리한 뒤, 열려있는 write객체 정리


---

# 클래스 설계

### 클래스 다이어그램
![partition_classDiagram](https://user-images.githubusercontent.com/12586821/61792870-ac256e00-ae58-11e9-910a-83d3a2c6b194.png)

---

### Producer 시퀀스 다이어그램
<img src="https://user-images.githubusercontent.com/12586821/61792850-a16ad900-ae58-11e9-8e29-6d849b8f2b12.png" width="500" height="300">

- 파일을 읽어 ValidatorUtil을 이용하여 유효성 검증
- 각 단어의 키값 생성하여 키/값 형태의 메세지로 매핑
- Broker에서 전송
- Broker는 Partitioner을 통해 각 메세지의 PartitionId을 할당
- 할당받은 파티션에 데이터를 저장하면서, 파티션의 메타정보 업데이트
    > 파티션 내 포함된 키 정보들


### consumer 시퀀스 다이어그램
<img src="https://user-images.githubusercontent.com/12586821/61792848-a0d24280-ae58-11e9-9776-cf8c48957c43.png" width="400" height="250">

- Broker에 메세지를 받아와, 키값에 해당되는 파일에 저장 

---
## (참고) words.txt 파일에 대한 리뷰

- **466,551개**의 단어 중 동일한 단어 없음.  
> - 유효하지 않은 단어 38건
> - 유효한 단어 466,513건

- 각 키 별 데이터 분포

| 단어 분포 정보 | 단어 수 |
|:--------------------------|--------:|
| a 또는 A로 시작하는 단어 |  30,851 |
| b 또는 B로 시작하는 단어 |  24,332 |
| c 또는 C로 시작하는 단어 |  38,933 |
| d 또는 D로 시작하는 단어 |  22,778 |
| e 또는 E로 시작하는 단어 |  16,815 |
| f 또는 F로 시작하는 단어 |  15,870 |
| g 또는 G로 시작하는 단어 |  14,466 |
| h 또는 H로 시작하는 단어 |  18,662 |
| i 또는 I로 시작하는 단어 |  15,220 |
| j 또는 J로 시작하는 단어 |   4,158 |
| k 또는 K로 시작하는 단어 |   6,167 |
| l 또는 L로 시작하는 단어 |  14,093 |
| m 또는 M로 시작하는 단어 |  25,194 |
| n 또는 N로 시작하는 단어 |  16,174 |
| o 또는 O로 시작하는 단어 |  15,061 |
| p 또는 P로 시작하는 단어 |  40,956 |
| q 또는 Q로 시작하는 단어 |   3,218 |
| r 또는 R로 시작하는 단어 |  21,286 |
| s 또는 S로 시작하는 단어 |  50,571 |
| t 또는 T로 시작하는 단어 |  25,223 |
| u 또는 U로 시작하는 단어 |  23,791 |
| v 또는 V로 시작하는 단어 |   6,816 |
| w 또는 W로 시작하는 단어 |  11,672 |
| x 또는 X로 시작하는 단어 |     609 |
| y 또는 Y로 시작하는 단어 |   1,740 |
| z 또는 Z로 시작하는 단어 |   1,822 |
| number로 시작하는 단어  |      35 |
| 총 단어수              | 466,513 |

