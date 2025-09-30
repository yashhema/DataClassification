-- ==========================================================================================
-- Stored Procedure: dbo.CommitBoundaryEnumeration
--
-- Description:
--   Atomically commits the work for a single completed enumeration boundary. It moves
--   all relevant records from a staging output table to the main task_output_records
--   table and creates a permanent checkpoint in the enumeration_progress table.
--   This entire operation is performed within a single transaction to ensure
--   data integrity. If any step fails, the entire operation is rolled back.
--
-- Parameters:
--   @task_id              - VARBINARY(32): The ID of the parent enumeration task.
--   @boundary_id          - VARBINARY(32): The unique hash of the boundary that was processed.
--   @boundary_path        - NVARCHAR(4000): The human-readable path for the boundary.
--   @output_staging_table - NVARCHAR(128): The name of the staging table to move records from.
--
-- Author:      System Architect
-- Create date: 2025-09-30
-- ==========================================================================================
CREATE OR ALTER PROCEDURE dbo.CommitBoundaryEnumeration
    @task_id VARBINARY(32),
    @boundary_id VARBINARY(32),
    @boundary_path NVARCHAR(4000),
    @output_staging_table NVARCHAR(128)
AS
BEGIN
    -- Ensures that if a statement raises a run-time error, the entire transaction is terminated and rolled back.
    SET XACT_ABORT ON;
    SET NOCOUNT ON;

    -- 1. Input Validation for Dynamic SQL Safety
    -- This prevents SQL injection by ensuring the provided table name is a valid, existing object.
    IF @output_staging_table IS NULL OR OBJECT_ID(@output_staging_table, 'U') IS NULL
    BEGIN
        RAISERROR('The specified staging table does not exist or is invalid.', 16, 1);
        RETURN -1; -- Return an error code
    END

    -- Safely quote the table name to prevent SQL injection in the dynamic SQL statements.
    DECLARE @SafeStagingTable NVARCHAR(258) = QUOTENAME(@output_staging_table);

    BEGIN TRY
        BEGIN TRANSACTION;

        -- 2. Move records from the staging table to the main task_output_records table.
        -- This is constructed as dynamic SQL to use the validated staging table name.
        DECLARE @MoveSql NVARCHAR(MAX);
        SET @MoveSql = N'
            INSERT INTO dbo.task_output_records (
                job_id,
                task_id,
                boundary_id,
                status,
                output_type,
                output_payload,
                created_timestamp
            )
            SELECT
                job_id,
                task_id,
                boundary_id,
                status,
                output_type,
                output_payload,
                created_timestamp
            FROM ' + @SafeStagingTable + N'
            WHERE task_id = @p_task_id AND boundary_id = @p_boundary_id;
        ';

        EXEC sp_executesql @MoveSql,
            N'@p_task_id VARBINARY(32), @p_boundary_id VARBINARY(32)',
            @p_task_id = @task_id,
            @p_boundary_id = @boundary_id;


        -- 3. Delete the moved records from the staging table to prevent reprocessing.
        DECLARE @DeleteSql NVARCHAR(MAX);
        SET @DeleteSql = N'
            DELETE FROM ' + @SafeStagingTable + N'
            WHERE task_id = @p_task_id AND boundary_id = @p_boundary_id;
        ';

        EXEC sp_executesql @DeleteSql,
            N'@p_task_id VARBINARY(32), @p_boundary_id VARBINARY(32)',
            @p_task_id = @task_id,
            @p_boundary_id = @boundary_id;


        -- 4. Insert a permanent checkpoint into the enumeration_progress table.
        -- This marks the boundary as fully processed and committed, enabling recovery.
        INSERT INTO dbo.enumeration_progress (
            task_id,
            boundary_id,
            boundary_path,
            status
            -- created_timestamp and last_updated use database defaults
        )
        VALUES (
            @task_id,
            @boundary_id,
            @boundary_path,
            'PROCESSING_COMPLETE' -- The initial state indicating the worker is done.
        );

        COMMIT TRANSACTION;

    END TRY
    BEGIN CATCH
        -- If any step in the transaction fails, roll back the entire operation.
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;

        -- Re-throw the original error so the calling application is aware of the failure.
        THROW;
    END CATCH
END
GO
