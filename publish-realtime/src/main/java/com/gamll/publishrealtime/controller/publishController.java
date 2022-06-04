package com.gamll.publishrealtime.controller;

import com.gamll.publishrealtime.bean.NameValue;
import com.gamll.publishrealtime.service.PublishsherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.Mapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import java.util.List;
import java.util.Map;

@Controller
public class publishController {
    @Autowired
    PublishsherService publishsherService;

    /**
     * 交易分析 - 按照类别（年龄、性别） 统计
     * http://bigdata.gmall.com/statsByItem?itemName=小米手机&date=2021-02-02&t=gender
     * http://bigdata.gmall.com/statsByItem?itemName=小米手机&date=2021-02-02&t=age
     */
    @ResponseBody
    @GetMapping("statsByItem")
    public List<NameValue> statsByItem(@RequestParam("itemName") String itemName,
                                       @RequestParam("date") String date,
                                       @RequestParam("t") String t) {

        List<NameValue> results = publishsherService.doStstsByItem(itemName, date, t);
        return results;
    }


    /**
     * 日活分析
     *
     * @param td
     * @return
     */
    @GetMapping("dauRealtime")
    @ResponseBody // 将结果以json字符串形式返回
    public Map<String, Object> dauRealtime(@RequestParam("td") String td) {
        Map<String, Object> results = publishsherService.doDauRealtime(td);
        return results;
    }
}
