# :video_game: Kakaogames2023

<img src="./images/카겜.jpeg" width="500" height="200"/>

카카오게임즈 산학 프로젝트

----------------------

## :book: 개요

### 주제

> MMORPG 게임 봇의 특징을 분석하고, 이를 탐지하는 모델 개발

### 기간
> 2023.07.01 ~ 2023.10.28

### 참여 인원
> 6명

### 역할
> 게임 로그 데이터 가공 및 분석, ExtraTrees 모델링

### 배경
> - 기존 봇 탐지 모델은 변화하는 작업장의 패턴에 대응하지 못하는 한계가 있어, 게임 운영에 지속적으로 활용할 수 있는 탐지 모델 필요
> - 게임 봇의 목적성 관점을 분석할 필요가 있음

### 목표
> - 기존 규칙 기반 시스템(rule-based)보다 봇을 예측하는 성능이 높은 모델 개발
> - 봇을 구분할 수 있는 행동 이외의 인사이트를 도출 

### 사용 데이터
> ‘O’게임의 2023년 3월 유저의 재화변화량 및 보유량, 아이템 거래 로그 데이터

## :chart_with_upwards_trend:분석 과정

1. Label 정의
- 운영팀에서 정의한 영구정지 유저를 정답(true label)으로 가정
- binary label

3. 데이터 가공
- 재화 보유량 변수: 분석 대상 유저의 마지막 로그로부터 일주일 간의 로그 데이터에 30분 간격으로 재화 보유량을 계산
- 아이템 시세 관련 변수: 시세보다 비싸게 구매/판매한 횟수, 수량, 거래가격과 시세 차이, 아이템 종류 수
  - 아이템 시세: 아이템의 3일간 거래 가격의 75%, 90% quantile을 시세로 정의(판매/구매)
  
3. 모델
  ExtraTrees, XGBoost, GradientBoosting 앙상블

