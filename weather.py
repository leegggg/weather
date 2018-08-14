# -*- coding: utf-8 -*-
import requests
from sqlalchemy import Column, String, create_engine, Integer, Float
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
import datetime
from datetime import timedelta
import time

Base = declarative_base()


# 定义User对象:
class Observation(Base):
    # 表的名字:
    __tablename__ = 'observation'

    # 表的结构:
    key = Column(String(255))  #  VDPP,
    pclass = Column(String(255))  #  observation,
    expire_time_gmt = Column(Integer, primary_key=True)  #  946722600,
    obs_id = Column(String(255))  #  VDPP,
    obs_name = Column(String(255))  #  Phnom-Penh,
    valid_time_gmt = Column(Integer, primary_key=True)  #  946715400,
    day_ind = Column(String(255))  #  D,
    temp = Column(Integer)  #  32,
    wx_icon = Column(Integer)  #  30,
    icon_extd = Column(Integer)  #  3000,
    wx_phrase = Column(String(255))  #  Partly Cloudy,
    pressure_tend = Column(String(255))  #  None,
    pressure_desc = Column(String(255))  #  None,
    dewPt = Column(Integer)  #  21,
    heat_index = Column(Integer)  #  34,
    rh = Column(Integer)  #  52,
    pressure = Column(Float)  #  1005.81,
    vis = Column(Integer)  #  9,
    wc = Column(Integer)  #  32,
    wdir = Column(Integer)  #  30,
    wdir_cardinal = Column(String(255))  #  NNE,
    gust = Column(String(255))  #  None,
    wspd = Column(Integer)  #  15,
    max_temp = Column(String(255))  #  None,
    min_temp = Column(String(255))  #  None,
    precip_total = Column(String(255))  #  None,
    precip_hrly = Column(String(255))  #  None,
    snow_hrly = Column(String(255))  #  None,
    uv_desc = Column(String(255))  #  None,
    feels_like = Column(Integer)  #  34,
    uv_index = Column(Integer)  #  -82,
    qualifier = Column(String(255))  #  None,
    qualifier_svrty = Column(String(255))  #  None,
    blunt_phrase = Column(String(255))  #  None,
    terse_phrase = Column(String(255))  #  None,
    clds = Column(String(255))  #  SCT,
    water_temp = Column(String(255))  #  None,
    primary_wave_period = Column(String(255))  #  None,
    primary_wave_height = Column(String(255))  #  None,
    primary_swell_period = Column(String(255))  #  None,
    primary_swell_height = Column(String(255))  #  None,
    primary_swell_direction = Column(String(255))  #  None,
    secondary_swell_period = Column(String(255))  #  None,
    secondary_swell_height = Column(String(255))  #  None,
    secondary_swell_direction = Column(String(255))  #  None,
    latitude = Column(Float, primary_key=True)  #  11.54,
    longitude = Column(Float, primary_key=True)  #  104.84


def dictToObsBoj(p):
    new_user = Observation(
        key=p.get("key"),
        pclass=p.get("class"),
        expire_time_gmt=p.get("expire_time_gmt"),
        obs_id=p.get("obs_id"),
        obs_name=p.get("obs_name"),
        valid_time_gmt=p.get("valid_time_gmt"),
        day_ind=p.get("day_ind"),
        temp=p.get("temp"),
        wx_icon=p.get("wx_icon"),
        icon_extd=p.get("icon_extd"),
        wx_phrase=p.get("wx_phrase"),
        pressure_tend=p.get("pressure_tend"),
        pressure_desc=p.get("pressure_desc"),
        dewPt=p.get("dewPt"),
        heat_index=p.get("heat_index"),
        rh=p.get("rh"),
        pressure=p.get("pressure"),
        vis=p.get("vis"),
        wc=p.get("wc"),
        wdir=p.get("wdir"),
        wdir_cardinal=p.get("wdir_cardinal"),
        gust=p.get("gust"),
        wspd=p.get("wspd"),
        max_temp=p.get("max_temp"),
        min_temp=p.get("min_temp"),
        precip_total=p.get("precip_total"),
        precip_hrly=p.get("precip_hrly"),
        snow_hrly=p.get("snow_hrly"),
        uv_desc=p.get("uv_desc"),
        feels_like=p.get("feels_like"),
        uv_index=p.get("uv_index"),
        qualifier=p.get("qualifier"),
        qualifier_svrty=p.get("qualifier_svrty"),
        blunt_phrase=p.get("blunt_phrase"),
        terse_phrase=p.get("terse_phrase"),
        clds=p.get("clds"),
        water_temp=p.get("water_temp"),
        primary_wave_period=p.get("primary_wave_period"),
        primary_wave_height=p.get("primary_wave_height"),
        primary_swell_period=p.get("primary_swell_period"),
        primary_swell_height=p.get("primary_swell_height"),
        primary_swell_direction=p.get("primary_swell_direction"),
        secondary_swell_period=p.get("secondary_swell_period"),
        secondary_swell_height=p.get("secondary_swell_height"),
        secondary_swell_direction=p.get("secondary_swell_direction"),
        latitude=p.get("latitude"),
        longitude=p.get("longitude"))
    return new_user


# 初始化数据库连接:
engine = create_engine('sqlite:///./sqlalchemy.db')
# 创建DBSession类型:
DBSession = sessionmaker(bind=engine)
session = DBSession()

# Base.metadata.create_all(engine)


def main():
    print("Main")
    date = datetime.datetime(1996, 11, 1)
    for i in range(5000):
        date = date + timedelta(days=27)
        dateStartString = "{:04d}{:02d}{:02d}".format(date.year, date.month,
                                                      date.day)
        dateEnd = date + timedelta(days=30)
        dateEndString = "{:04d}{:02d}{:02d}".format(dateEnd.year,
                                                    dateEnd.month, dateEnd.day)
        mt = {}
        obs = {}
        times = 0
        while (True):
            times = times + 1
            response = requests.get(
                "https://api.weather.com/v1/geocode/11.54388905/104.84722137/observations/historical.json"
                + "?apiKey=6532d6454b8aa370768e63d6ba5a832e&units=m" +
                "&startDate=" + dateStartString + "&endDate=" +
                dateEndString).json()
            mt = response["metadata"]

            if mt and mt.get("status_code") < 300:
                obs = response["observations"]
                break
            else:
                # time.sleep(1)
                print(response)

            if times > 1:
                break

        print("{}-{}  {}".format(dateStartString, dateEndString, len(obs)))

        for p in obs:
            p["latitude"] = mt["latitude"]
            p["longitude"] = mt["longitude"]
            obsP = dictToObsBoj(p)
            insert(obsP)

    return


def insert(obs):
    try:
        session.add(obs)
        # 提交即保存到数据库:
        session.commit()
    except:
        pass
    session.close()
    # 关闭session:
    return


if __name__ == "__main__":
    main()
