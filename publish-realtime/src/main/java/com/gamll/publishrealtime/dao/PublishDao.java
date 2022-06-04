package com.gamll.publishrealtime.dao;

import com.gamll.publishrealtime.bean.NameValue;

import java.util.List;
import java.util.Map;

public interface PublishDao {
    Map<String, Object> searchDau(String td);

  public abstract   List<NameValue>  searchStatsByItem(String itemName, String date, String field);
}
