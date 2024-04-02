use std::{marker::PhantomData, ops::ControlFlow, sync::Arc};

use anyhow::{Context, Result};
use scylla::{frame::response::result::CqlValue, prepared_statement::PreparedStatement, Session};


use crate::settings::CassandraStressSettings;

use super::{
    row_generator::RowGenerator, CassandraStressOperation, CassandraStressOperationFactory,
    EqualRowValidator, ExistsRowValidator, RowValidator,
};

pub struct ReadOperation<V: RowValidator> {
    session: Arc<Session>,
    statement: PreparedStatement,
    row_validator: V,
}

pub struct GenericReadOperationFactory<V: RowValidator> {
    session: Arc<Session>,
    statement: PreparedStatement,
    _phantom: PhantomData<V>,
}

pub type RegularReadOperation = ReadOperation<EqualRowValidator>;
pub type RegularReadOperationFactory = GenericReadOperationFactory<EqualRowValidator>;

pub type CounterReadOperation = ReadOperation<ExistsRowValidator>;
pub type CounterReadOperationFactory = GenericReadOperationFactory<ExistsRowValidator>;

impl<V: RowValidator> ReadOperation<V> {
    async fn do_execute(&self, row: &[CqlValue]) -> Result<ControlFlow<()>> {
        // This is the key/val we expect.
        let pk = &row[0];

        let result = self.session.execute(&self.statement, (pk,)).await;
        if let Err(err) = result.as_ref() {
            tracing::error!(
                error = %err,
                partition_key = ?pk,
                "read error",
            );
        }

        let validation_result = self.row_validator.validate_row(row, result?);
        if let Err(err) = validation_result.as_ref() {
            tracing::error!(
                error = %err,
                partition_key = ?pk,
                "read validation error",
            );
        }
        validation_result
            .with_context(|| format!("Row with partition_key: {:?} could not be validated.", pk))?;

        Ok(ControlFlow::Continue(()))
    }

    async fn do_execute_in(&self, mut row: Vec<Vec<CqlValue>>) -> Result<ControlFlow<()>> {
        let mut key = Vec::new();
        // TODO validate? Commented as no-op atm
        // let mut val = Vec::new();

        for i in row.iter_mut() {
            key.push(&i[0]);
            // val.push(&i[1]);
        }

        let result = self.session.execute(&self.statement, (key.clone(),)).await?;
        
        // Rather than validating the result, we want to know if we got the expected number of
        // rows. If we didn't, then we know we've hit a missing key.
        //
        // Note, we don't do any checking on the number of operations "left to complete". 
        let mut rowcount = 0;
        for _row in result.rows.unwrap() {
            rowcount += 1;
        }

        if rowcount != 16 {
            println!("Expected 16 rows, but got {} rows? Key list: {:?}", rowcount, key);
        }
        
        Ok(ControlFlow::Continue(()))
    }
}

impl<V: RowValidator> CassandraStressOperation for ReadOperation<V> {
    type Factory = GenericReadOperationFactory<V>;

    async fn execute(&self, row: &[CqlValue]) -> Result<ControlFlow<()>> {
        self.do_execute(row).await
    }

    async fn execute_in(&self, rows: Vec<Vec<CqlValue>>) -> Result<ControlFlow<()>> {
        self.do_execute_in(rows).await
    }

    fn generate_row(&self, row_generator: &mut RowGenerator) -> Vec<CqlValue> {
        row_generator.generate_row()
    }
}

impl<V: RowValidator> CassandraStressOperationFactory for GenericReadOperationFactory<V> {
    type Operation = ReadOperation<V>;

    fn create(&self) -> Self::Operation {
        ReadOperation {
            session: Arc::clone(&self.session),
            statement: self.statement.clone(),
            row_validator: Default::default(),
        }
    }
}

impl<V: RowValidator> GenericReadOperationFactory<V> {
    pub async fn new(
        settings: Arc<CassandraStressSettings>,
        session: Arc<Session>,
        stressed_table_name: &'static str,
    ) -> Result<Self> {
        let statement_str = format!("SELECT * FROM {} WHERE KEY IN ?", stressed_table_name);
        let mut statement = session
            .prepare(statement_str)
            .await
            .context("Failed to prepare statement")?;

        statement.set_is_idempotent(true);
        statement.set_consistency(settings.command_params.common.consistency_level);
        statement.set_serial_consistency(Some(
            settings.command_params.common.serial_consistency_level,
        ));

        Ok(Self {
            session,
            statement,
            _phantom: PhantomData,
        })
    }
}
