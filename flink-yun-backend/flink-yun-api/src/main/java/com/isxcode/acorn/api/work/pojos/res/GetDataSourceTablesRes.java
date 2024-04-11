package com.isxcode.acorn.api.work.pojos.res;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class GetDataSourceTablesRes {

	private List<String> tables;
}
