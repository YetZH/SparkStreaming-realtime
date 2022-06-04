package com.gamll.publishrealtime.service.impl;

import com.gamll.publishrealtime.bean.NameValue;
import com.gamll.publishrealtime.dao.PublishDao;
import com.gamll.publishrealtime.service.PublishsherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Service
public class PublisherServiceImpl implements PublishsherService {
    @Autowired
    PublishDao publishDao;

    /**
     * 日活分析业务处理
     */
    @Override
    public Map<String, Object> doDauRealtime(String td) {
        Map<String, Object> dauResult = publishDao.searchDau(td);
        return dauResult;
    }

    /**
     * 交易分析业务处理
     */
    @Override
    public List<NameValue> doStstsByItem(String itemName, String date, String t) {
        String field = typeTofield(t);
        List<NameValue> searchResults = publishDao.searchStatsByItem(itemName, date, field);

        List<NameValue> results = transformResults(searchResults, t);
        return results;
    }

    private List<NameValue> transformResults(List<NameValue> searchResults, String t) {
        if ("gender".equals(t)) {
            if (searchResults.size() > 0) {
                for (NameValue searchResult : searchResults) {
                    String name = searchResult.getName();
                    if ("F".equals(name)) {
                        searchResult.setName("女");
                    } else if ("M".equals(name)) {
                        searchResult.setName("男");

                    } else searchResult.setName("人妖");
                }

            }
            return searchResults;

        } else if ("age".equals(t)) {
            List<NameValue> result = new ArrayList<>();
            double totalsmall = 0;
            double totalmid = 0;
            double totalbig = 0;

            if (searchResults.size() > 0) {
                for (NameValue searchResult : searchResults) {
                    double value = (double) searchResult.getValue();
                    int name = Integer.parseInt(searchResult.getName());
                    if (name < 20) totalsmall+=value;
                    if (name >= 20 && name <=29) totalmid+=value;
                    if (name >= 30)totalbig+=value;
                }
                searchResults.clear();
                searchResults.add( new NameValue("20岁以下", totalsmall));
                searchResults.add( new NameValue("20岁至29岁", totalmid));
                searchResults.add( new NameValue("30岁以上", totalbig));
            }
            return searchResults;

        }
    else return null;
    }


    private String  typeTofield(String t) {
        if ("age".equals(t)) return "user_age";
        else if ("gender".equals(t)) return "user_gender.keyword";
        else return null;

    }
}
