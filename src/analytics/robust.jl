import Pkg
using Distributions
using Ipopt
using JuMP
using JuMPeR
using LinearAlgebra
using Printf
using Random
using Statistics

# Pkg.add('Mosek')

Random.seed!(1)
const UNCERTAINY_SET = :Ellipsoidal
const UNCERTAINY_SET = :Polyhedral
const NUM_SAMPLES = 1000
const NUM_ASSET = 25


function solve_portfolio(past_returns, Γ)
    m = RobustModel(solver=IpoptSolver())
    @variable(m, obj)
    @variable(m, 0 <= x[1:NUM_ASSET] <= 1)
    @objective(m, Max, obj)
    μ = vec(mean(past_returns, dims=1))
    Σ = cov(past_returns)
    L = Matrix(cholesky(Σ))
    @uncertain(m, r[1:NUM_ASSET])
    @uncertain(m, z[1:NUM_ASSET])
    @constraint(m, sum(x) == 1)
    @constraint(m, r .== L*z + μ)
    @constraint(m, norm(z,   2) <= Γ)
    @constraint(m, obj <= dot(r, x))
    solve(m)
    return getvalue(x), getvalue(obj)
end

function generate_data()
    data = zeros(NUM_SAMPLES, NUM_ASSET)
    # Linking factors to induce correlations
    β = [(i-1.0)/NUM_ASSET for i in 1:NUM_ASSET]
    for i in 1:NUM_SAMPLES
        # Common market factor, μ=3%, σ=5%, truncated at ±3σ
        z = rand(Normal(0.03, 0.05))
        z = max(z, 0.03 - 3*0.05)
        z = min(z, 0.03 + 3*0.05)
        for j in 1:NUM_ASSET
            # Idiosyncratic contribution, μ=0%, σ=5%, truncated at ±3σ
            r = rand(Normal(0.00, 0.05))
            r = max(r, 0.00 - 3*0.05)
            r = min(r, 0.00 + 3*0.05)
            data[i,j] = β[j] * z + r
        end
    end
    return data
end


function solve_and_simulate()
    past_returns   = generate_data()
    future_returns = generate_data()
    # Generate and store the portfolios
    portfolios = Dict()
    for Γ in 0:NUM_ASSET
        x, _ = solve_portfolio(past_returns, Γ)
        portfolios[Γ] = x
    end
    # Print table header
    println("    Γ |    Min |    10% |    20% |   Mean |    Max |  Sharpe| Support")
    # Evaluate and print distributions
    for Γ in 0:NUM_ASSET
        # Evaluate portfolio returns in future
        future_z = future_returns * portfolios[Γ]
        sort!(future_z)
        min_z    = future_z[1]*100
        ten_z    = future_z[div(NUM_SAMPLES,10)]*100
        twenty_z = future_z[div(NUM_SAMPLES, 5)]*100
        mean_z   = mean(future_z)*100
        std_z = std(future_z) * 100
        sharpe_z = 16 * mean_z / std_z
        max_z    = future_z[end]*100
        support  = sum(portfolios[Γ] .> 0.01)  # Number of assets used in portfolio
        @printf(" %4.1f | %6.2f | %6.2f | %6.2f | %6.2f |  %6.2f | %6.2f | %7d\n",
                    Γ, min_z, ten_z, twenty_z, mean_z, max_z, sharpe_z, support)
    end
end
