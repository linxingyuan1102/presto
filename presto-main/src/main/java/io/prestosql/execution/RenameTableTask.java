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
package io.prestosql.execution;

import com.google.common.util.concurrent.ListenableFuture;
import io.prestosql.Session;
import io.prestosql.execution.warnings.WarningCollector;
import io.prestosql.metadata.Metadata;
import io.prestosql.metadata.QualifiedObjectName;
import io.prestosql.metadata.TableHandle;
import io.prestosql.security.AccessControl;
import io.prestosql.spi.PrestoWarning;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.RenameTable;
import io.prestosql.transaction.TransactionManager;

import java.util.List;
import java.util.Map.Entry;
import java.util.Optional;

import static com.google.common.util.concurrent.Futures.immediateFuture;
import static io.prestosql.metadata.MetadataUtil.createQualifiedObjectName;
import static io.prestosql.spi.StandardErrorCode.CATALOG_NOT_FOUND;
import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.prestosql.spi.StandardErrorCode.TABLE_ALREADY_EXISTS;
import static io.prestosql.spi.StandardErrorCode.TABLE_NOT_FOUND;
import static io.prestosql.spi.connector.StandardWarningCode.REDIRECTED_TABLE;
import static io.prestosql.sql.analyzer.SemanticExceptions.semanticException;

public class RenameTableTask
        implements DataDefinitionTask<RenameTable>
{
    @Override
    public String getName()
    {
        return "RENAME TABLE";
    }

    @Override
    public ListenableFuture<?> execute(
            RenameTable statement,
            TransactionManager transactionManager,
            Metadata metadata,
            AccessControl accessControl,
            QueryStateMachine stateMachine,
            List<Expression> parameters,
            WarningCollector warningCollector)
    {
        Session session = stateMachine.getSession();
        QualifiedObjectName sourceTableName = createQualifiedObjectName(session, statement, statement.getSource());
        QualifiedObjectName targetTableName = createQualifiedObjectName(session, statement, statement.getTarget());
        Optional<Entry<QualifiedObjectName, QualifiedObjectName>> redirectedSourceTargetTables = metadata.redirectTableRename(session, sourceTableName, targetTableName);
        if (redirectedSourceTargetTables.isPresent()) {
            sourceTableName = redirectedSourceTargetTables.get().getKey();
            targetTableName = redirectedSourceTargetTables.get().getValue();
            warningCollector.add(new PrestoWarning(REDIRECTED_TABLE, "Table redirection happened"));
        }

        Optional<TableHandle> tableHandle = metadata.getTableHandle(session, sourceTableName);
        if (tableHandle.isEmpty()) {
            if (!statement.isExists()) {
                throw semanticException(TABLE_NOT_FOUND, statement, "Table '%s' does not exist", sourceTableName);
            }
            return immediateFuture(null);
        }

        if (metadata.getCatalogHandle(session, targetTableName.getCatalogName()).isEmpty()) {
            throw semanticException(CATALOG_NOT_FOUND, statement, "Target catalog '%s' does not exist", targetTableName.getCatalogName());
        }
        if (metadata.getTableHandle(session, targetTableName).isPresent()) {
            throw semanticException(TABLE_ALREADY_EXISTS, statement, "Target table '%s' already exists", targetTableName);
        }
        if (!sourceTableName.getCatalogName().equals(targetTableName.getCatalogName())) {
            throw semanticException(NOT_SUPPORTED, statement, "Table rename across catalogs is not supported");
        }
        accessControl.checkCanRenameTable(session.toSecurityContext(), sourceTableName, targetTableName);

        metadata.renameTable(session, tableHandle.get(), targetTableName);

        return immediateFuture(null);
    }
}
