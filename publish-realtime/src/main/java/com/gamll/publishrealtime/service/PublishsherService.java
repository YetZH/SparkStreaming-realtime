package com.gamll.publishrealtime.service;


import com.gamll.publishrealtime.bean.NameValue;

import java.util.List;
import java.util.Map;

public interface PublishsherService {
//    日活分析业务处理
    Map<String, Object> doDauRealtime(String td);
//交易分析业务处理
    List<NameValue> doStstsByItem(String itemName, String date, String t);
}
