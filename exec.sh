#!/bin/bash

PKG1="za.co.absa:abris_2.11:3.2.1"
PKG2="org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.4"
spark-submit --packages "${PKG1},${PKG2}" main/AbrisSample.py
