using QueryDF
using DataFrames
using Test
using Dates
using DuckDB
using Logging

# Silence debug/info logs during tests
global_logger(ConsoleLogger(stderr, Logging.Warn))

@testset "QueryDF.jl" begin

    # ── QueryConfig ───────────────────────────────────────────────────────────
    @testset "QueryConfig defaults" begin
        cfg = QueryConfig()
        @test cfg.threads == 0
        @test cfg.memory_limit == ""
        @test cfg.readonly == false
        @test cfg.on_error == :throw
        @test cfg.extensions == String[]
        @test cfg.init_sql == String[]
    end

    @testset "QueryConfig validation" begin
        @test_throws ArgumentError QueryConfig(on_error=:invalid)
    end

    # ── Basic in-memory queries ───────────────────────────────────────────────
    @testset "In-memory SELECT" begin
        ctx = QueryContext()
        df = execute(ctx, "SELECT 42 AS answer, 'hello' AS greeting")
        @test size(df) == (1, 2)
        @test df[1, :answer] == 42
        @test df[1, :greeting] == "hello"
        close!(ctx)
    end

    @testset "with_context scoping" begin
        result = with_context() do ctx
            execute(ctx, "SELECT 1 + 1 AS two")
        end
        @test result[1, :two] == 2
    end

    # ── QueryResult metadata ──────────────────────────────────────────────────
    @testset "QueryResult" begin
        ctx = QueryContext()
        r = query(ctx, "SELECT 7 AS n")
        @test r isa QueryResult
        @test nrow(r) == 1
        @test r[1, :n] == 7
        @test r.elapsed_ns > 0
        @test elapsed_ms(r) >= 0.0
        @test DataFrame(r) isa DataFrame
        close!(ctx)
    end

    # ── DataFrame source registration ─────────────────────────────────────────
    @testset "register! DataFrame" begin
        ctx = QueryContext()
        customers = DataFrame(id=1:3, name=["Alice", "Bob", "Charlie"])
        register!(ctx, "customers", customers)

        df = execute(ctx, "SELECT * FROM customers WHERE id > 1 ORDER BY id")
        @test size(df) == (2, 2)
        @test df[1, :name] == "Bob"
        @test df[2, :name] == "Charlie"
        close!(ctx)
    end

    @testset "register! multiple sources with pair syntax" begin
        ctx = QueryContext()
        orders    = DataFrame(id=1:4, customer_id=[1,1,2,3], amount=[100.0,200.0,150.0,300.0])
        customers = DataFrame(id=1:3, name=["Alice","Bob","Charlie"])
        register!(ctx, "orders" => orders, "customers" => customers)

        df = execute(ctx, """
            SELECT c.name, SUM(o.amount) AS total
            FROM orders o
            JOIN customers c ON o.customer_id = c.id
            GROUP BY c.name
            ORDER BY total DESC
        """)
        @test size(df, 1) == 3
        @test df[1, :name] == "Alice"    # Alice has 100+200=300
        @test df[1, :total] == 300.0
        close!(ctx)
    end

    @testset "list_sources and deregister!" begin
        ctx = QueryContext()
        df = DataFrame(x=1:5)
        register!(ctx, "data", df)

        sources = list_sources(ctx)
        @test nrow(sources) == 1
        @test sources[1, :name] == "data"
        @test sources[1, :type] == "DataFrame"

        deregister!(ctx, "data")
        @test isempty(ctx.sources)
        close!(ctx)
    end

    # ── Parameterized queries ─────────────────────────────────────────────────
    @testset "Positional parameters" begin
        ctx = QueryContext()
        df_src = DataFrame(id=1:5, score=[10,20,30,40,50])
        register!(ctx, "t", df_src)

        df = execute(ctx, "SELECT * FROM t WHERE score > ? ORDER BY score", 25)
        @test size(df, 1) == 3
        @test df[1, :score] == 30

        # Multiple positional params
        df2 = execute(ctx, "SELECT * FROM t WHERE score >= ? AND score <= ?", 20, 40)
        @test size(df2, 1) == 3
        close!(ctx)
    end

    @testset "Named parameters" begin
        ctx = QueryContext()
        df_src = DataFrame(id=1:5, name=["a","b","c","d","e"], val=[10,20,30,40,50])
        register!(ctx, "t", df_src)

        df = execute(ctx, "SELECT * FROM t WHERE val > :min AND val < :max", min=15, max=45)
        @test size(df, 1) == 3   # 20, 30, 40

        df2 = execute(ctx, "SELECT * FROM t WHERE name = :nm", nm="c")
        @test nrow(df2) == 1
        @test df2[1, :val] == 30
        close!(ctx)
    end

    @testset "Mixed params error" begin
        ctx = QueryContext()
        @test_throws QueryError execute(ctx, "SELECT ? + :x", 1; x=2)
        close!(ctx)
    end

    @testset "Missing named param error" begin
        ctx = QueryContext()
        @test_throws QueryError execute(ctx, "SELECT :missing_param")
        close!(ctx)
    end

    # ── Batch execution ────────────────────────────────────────────────────────
    @testset "execute with Vector{String}" begin
        ctx = QueryContext()
        df = execute(ctx, [
            "CREATE TABLE nums AS SELECT generate_series AS n FROM generate_series(1, 10)",
            "SELECT SUM(n) AS total FROM nums"
        ])
        @test df[1, :total] == 55
        close!(ctx)
    end

    # ── execute! (DDL / DML) ──────────────────────────────────────────────────
    @testset "execute! returns nothing" begin
        ctx = QueryContext()
        result = execute!(ctx, "CREATE TABLE scratch (x INT)")
        @test result === nothing
        execute!(ctx, "INSERT INTO scratch VALUES (42)")
        df = execute(ctx, "SELECT x FROM scratch")
        @test df[1, :x] == 42
        close!(ctx)
    end

    # ── Transactions ──────────────────────────────────────────────────────────
    @testset "Transaction commit" begin
        ctx = QueryContext()
        execute!(ctx, "CREATE TABLE accounts (id INT, balance DOUBLE)")
        execute!(ctx, "INSERT INTO accounts VALUES (1, 1000.0)")
        execute!(ctx, "INSERT INTO accounts VALUES (2, 2000.0)")

        transaction(ctx) do tx
            execute!(tx, "UPDATE accounts SET balance = balance - 100 WHERE id = 1")
            execute!(tx, "UPDATE accounts SET balance = balance + 100 WHERE id = 2")
        end

        df = execute(ctx, "SELECT balance FROM accounts ORDER BY id")
        @test df[1, :balance] == 900.0
        @test df[2, :balance] == 2100.0
        close!(ctx)
    end

    @testset "Transaction rollback on error" begin
        ctx = QueryContext()
        execute!(ctx, "CREATE TABLE log_tbl (msg VARCHAR)")

        @test_throws Exception transaction(ctx) do tx
            execute!(tx, "INSERT INTO log_tbl VALUES ('first')")
            error("Deliberate error to trigger rollback")
        end

        # Row should have been rolled back
        df = execute(ctx, "SELECT COUNT(*) AS n FROM log_tbl")
        @test df[1, :n] == 0
        close!(ctx)
    end

    # ── Streaming ──────────────────────────────────────────────────────────────
    @testset "stream in batches" begin
        ctx = QueryContext()
        execute!(ctx, "CREATE TABLE big AS SELECT generate_series AS n FROM generate_series(1, 100)")

        batches = collect(stream(ctx, "SELECT * FROM big ORDER BY n"; batch_size=30))
        @test length(batches) == 4    # 30+30+30+10
        @test sum(nrow, batches) == 100
        @test batches[1][1, :n] == 1
        close!(ctx)
    end

    # ── Error handling modes ──────────────────────────────────────────────────
    @testset "on_error = :throw" begin
        ctx = QueryContext(on_error=:throw)
        @test_throws QueryError execute(ctx, "SELECT * FROM nonexistent_table_xyz")
        close!(ctx)
    end

    @testset "on_error = :empty" begin
        ctx = QueryContext(on_error=:empty)
        df = execute(ctx, "SELECT * FROM nonexistent_table_xyz")
        @test df isa DataFrame
        @test nrow(df) == 0
        close!(ctx)
    end

    # ── explain ───────────────────────────────────────────────────────────────
    @testset "explain" begin
        ctx = QueryContext()
        plan = explain(ctx, "SELECT 1 + 1")
        @test plan isa String
        @test !isempty(plan)
        close!(ctx)
    end

    # ── QueryResult – Tables.jl interface ────────────────────────────────────
    @testset "QueryResult Tables.jl interface" begin
        ctx = QueryContext()
        r = query(ctx, "SELECT 1 AS a, 2 AS b")
        @test Tables.istable(typeof(r))
        @test Tables.columnaccess(typeof(r))
        cols = Tables.columns(r)
        @test Tables.getcolumn(cols, :a) == [1]
        close!(ctx)
    end

    # ── Connection pool ───────────────────────────────────────────────────────
    @testset "ConnectionPool basic" begin
        pool = ConnectionPool(":memory:"; size=2)
        conn = acquire!(pool)
        @test conn isa DuckDB.DB
        release!(pool, conn)
        close!(pool)
    end

    @testset "with_connection" begin
        pool = ConnectionPool(":memory:"; size=2)
        result = with_connection(pool) do conn
            DuckDB.execute(conn, "SELECT 99 AS n") |> DataFrame
        end
        @test result[1, :n] == 99
        close!(pool)
    end

    # ── File persistence ──────────────────────────────────────────────────────
    @testset "File-based database" begin
        db_file = tempname() * ".duckdb"
        try
            ctx1 = QueryContext(db_file)
            execute!(ctx1, "CREATE TABLE items (id INT, label VARCHAR)")
            execute!(ctx1, "INSERT INTO items VALUES (1, 'foo'), (2, 'bar')")
            close!(ctx1)

            ctx2 = QueryContext(db_file)
            df = execute(ctx2, "SELECT * FROM items ORDER BY id")
            @test size(df) == (2, 2)
            @test df[2, :label] == "bar"
            close!(ctx2)
        finally
            isfile(db_file) && rm(db_file)
        end
    end

    # ── Closed context guard ─────────────────────────────────────────────────
    @testset "Error on closed context" begin
        ctx = QueryContext()
        close!(ctx)
        @test_throws QueryError execute(ctx, "SELECT 1")
    end

    # ── init_sql runs on connection open ──────────────────────────────────────
    @testset "init_sql" begin
        ctx = QueryContext(":memory:"; init_sql=[
            "CREATE TABLE init_check AS SELECT 'ran' AS status"
        ])
        df = execute(ctx, "SELECT status FROM init_check")
        @test df[1, :status] == "ran"
        close!(ctx)
    end

    # ── Boolean / Date / DateTime types ───────────────────────────────────────
    @testset "Mixed column types in DataFrame source" begin
        ctx = QueryContext()
        df_src = DataFrame(
            id       = 1:3,
            flag     = [true, false, true],
            score    = [1.5, 2.5, 3.5],
            name     = ["x", "y", "z"],
            on_date  = [Date(2024,1,1), Date(2024,6,1), Date(2024,12,31)]
        )
        register!(ctx, "mixed", df_src)
        df = execute(ctx, "SELECT * FROM mixed WHERE flag = TRUE ORDER BY id")
        @test size(df, 1) == 2
        @test df[1, :name] == "x"
        close!(ctx)
    end

    # ── DuckDB-incompatible column types (Vector, Symbol, Missing) ─────────────
    @testset "DataFrame with incompatible column types" begin
        ctx = QueryContext()
        # Replicate the real-world case from the bug report:
        # Vector{Float64}, Symbol, and all-Missing columns
        df_src = DataFrame(
            id              = Int32[1, 2, 3],
            power_phase     = Union{Vector{Float64}, Missing}[
                                  [326.25, 260.156],
                                  [329.063, 258.75],
                                  missing],
            cadence         = UInt8[41, 35, 37],
            message_name    = Symbol[:record, :record, :record],
            left_smoothness = Missing[missing, missing, missing]
        )

        # Registration must not throw
        @test_nowarn register!(ctx, "sensor", df_src)

        df = execute(ctx, "SELECT id, cadence FROM sensor ORDER BY id")
        @test nrow(df) == 3
        @test df[1, :id] == 1
        @test df[2, :cadence] == 35

        # Vector columns serialized to string
        df2 = execute(ctx, "SELECT power_phase FROM sensor WHERE id = 1")
        @test df2[1, :power_phase] == "[326.25, 260.156]"

        # Symbol column serialized to string
        df3 = execute(ctx, "SELECT message_name FROM sensor WHERE id = 2")
        @test df3[1, :message_name] == "record"

        # Pure-missing column registered without error; values are NULL
        df4 = execute(ctx, "SELECT left_smoothness FROM sensor")
        @test all(ismissing, df4[!, :left_smoothness])

        close!(ctx)
    end

    # ── _needs_sanitization helper ────────────────────────────────────────────
    @testset "_needs_sanitization" begin
        using QueryDF: _needs_sanitization
        @test  _needs_sanitization(Vector{Float64})
        @test  _needs_sanitization(Symbol)
        @test  _needs_sanitization(Missing)
        @test  _needs_sanitization(Union{Vector{Float64}, Missing})
        @test !_needs_sanitization(Int32)
        @test !_needs_sanitization(UInt8)
        @test !_needs_sanitization(Float64)
        @test !_needs_sanitization(String)
        @test !_needs_sanitization(Bool)
        @test !_needs_sanitization(Union{Int64, Missing})
        @test !_needs_sanitization(Union{String, Missing})
    end

end  # @testset "QueryDF.jl"
