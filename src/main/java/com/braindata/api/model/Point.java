package com.braindata.api.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author gongchangyou
 * @version 1.0
 * @date 2022/3/15 1:23 下午
 */
@Builder
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Point {
    private Double x;
    private Double y;
    private Integer count;

}
