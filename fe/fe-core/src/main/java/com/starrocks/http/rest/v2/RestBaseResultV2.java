// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.starrocks.http.rest.v2;

import com.google.gson.ExclusionStrategy;
import com.google.gson.FieldAttributes;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.annotations.SerializedName;
import com.starrocks.http.rest.RestBaseResult;

import java.util.Objects;

public class RestBaseResultV2<T> extends RestBaseResult {

    @SerializedName("result")
    private T result;

    public RestBaseResultV2() {
    }

    public RestBaseResultV2(String msg) {
        super(msg);
    }

    public RestBaseResultV2(T result) {
        super();
        this.result = result;
    }

    public RestBaseResultV2(int code, String message) {
        this(Objects.toString(code), message);
    }

    public RestBaseResultV2(String code, String message) {
        this.code = code;
        this.message = message;
    }

    @Override
    public String toJson() {
        Gson gson = new GsonBuilder()
                .setExclusionStrategies(new ExclusionStrategy() {
                    @Override
                    public boolean shouldSkipField(FieldAttributes f) {
                        return f.getAnnotation(Legacy.class) != null;
                    }

                    @Override
                    public boolean shouldSkipClass(Class<?> clazz) {
                        return false;
                    }
                }).create();
        return gson.toJson(this);
    }

    public T getResult() {
        return result;
    }
}
