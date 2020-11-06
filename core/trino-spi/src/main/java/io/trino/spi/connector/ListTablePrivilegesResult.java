/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.spi.connector;

import io.trino.spi.security.GrantInfo;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class ListTablePrivilegesResult
{
    private final SchemaTableName tableName;
    private final Optional<GrantInfo> grantInfo; // An Optional.empty() value means the table is redirected

    public ListTablePrivilegesResult(SchemaTableName tableName, Optional<GrantInfo> grantInfo)
    {
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.grantInfo = requireNonNull(grantInfo, "grantInfo is null");
    }

    public SchemaTableName getTableName()
    {
        return tableName;
    }

    public Optional<GrantInfo> getGrantInfo()
    {
        return grantInfo;
    }
}
