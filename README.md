# :video_game: 재화 기반 게임 봇 탐지 분석:video_game:

<img src="./kakaogames_image2023/카겜.jpeg" width="500" height="150"/>

**2023 카카오게임즈 산학 프로젝트**

----------------------

## :book: 개요

### :dart:주제

- MMORPG 게임 봇의 특징을 분석하고, 이를 탐지하는 모델 개발
- 게임 데이터 분석 및 모델링을 배우는 것에 의의 :pencil2:

### :calendar:기간
2023.07.01 ~ 2023.10.28

### :busts_in_silhouette:참여 인원
6명

### :memo:역할
ExtraTrees 모델링, 성능 향상을 위한 변수 조합

### :chart_with_upwards_trend:배경
- 기존 봇 탐지 모델은 변화하는 작업장의 패턴에 대응하지 못하는 한계가 있어, 게임 운영에 지속적으로 활용할 수 있는 탐지 모델 필요
- 게임 봇의 목적성 관점을 분석할 필요가 있음

### :triangular_flag_on_post:목표
- 기존 규칙 기반 시스템(rule-based)보다 봇을 예측하는 성능이 높은 모델 개발
- 봇을 구분할 수 있는 행동 이외의 인사이트를 도출 

### :open_file_folder:사용 데이터
‘O’게임의 2023년 3월 유저의 재화변화량 및 보유량, 아이템 거래 로그 데이터


### :bulb: 문제 정의
- 봇의 행동 이외의 특징을 파악해 새로운 변수 발굴 필요
- 정상유저보다 봇이 더 적은 불균형 문제 존재

### :crown:성과

- **안정적인 성능:** 앙상블 모델로 robust한 모델 구축
- **범용성:** 재화의 패턴과 아이템 시세를 고려한 모델을 제시하여 타 게임 환경에도 쉽게 적용 가능
- **수익성:** 봇의 현금 수취로 인한 게임사의 손실을 줄일 수 있으며, 정상유저의 이탈 방지


---------

## :chart_with_upwards_trend:분석 과정

1. Label 정의
- 운영팀에서 정의한 영구정지 유저를 정답(true label)으로 가정
- binary label

2. 데이터 가공
- 재화 보유량 변수: 분석 대상 유저의 마지막 로그로부터 일주일 간의 로그 데이터에 30분 간격으로 재화 보유량을 계산
- 아이템 시세 관련 변수: 시세보다 비싸게 구매/판매한 횟수, 수량, 거래가격과 시세 차이, 아이템 종류 수
  - 아이템 시세: 아이템의 3일간 거래 가격의 75%, 90% quantile을 시세로 정의(판매/구매)
  
3. 모델
- ExtraTrees, XGBoost, GradientBoosting을 10-fold cv로 적합하고, 각 모델의 f1 score의 평균을 이용해 가중치 정의
- ExtraTrees, XGBoost, GradientBoosting의 가중평균 앙상블 모델

4. 결과

||Accuracy|Precision|Recall|F1-score|
|:---:|:---:|:---:|:---:|:---:|
|Test|0.9158|0.7164|0.6564|**0.6857**|

<img src="./kakaogames_image2023/취업 포트폴리오 최종_5.png" width="800" height="400"/>
<img src="./kakaogames_image2023/취업 포트폴리오 최종_6.png" width="800" height="400"/>
