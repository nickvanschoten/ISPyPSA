import pypsa
from ispypsa.nextgen.mga_ai.surrogate_abc import MGAConstraintGenerator

class PyPSAMGAConstraintGenerator(MGAConstraintGenerator):
    """
    Implements standard Modeling to Generate Alternatives (MGA) in PyPSA.
    """
    
    def generate_slack_constraints(self, network: pypsa.Network, optimal_cost: float, slack_pct: float) -> None:
        """
        Add constraints to the PyPSA network objective function ensuring new solutions
        do not exceed optimal_cost * (1 + slack_pct).
        
        This should be called *after* an initial `network.optimize(..., return_cost=True)` 
        and before subsequent alternative generation passes.
        """
        import linopy
        
        # Get the objective cost expression from PyPSA's linopy model
        if not hasattr(network, "model"):
            raise ValueError("Network has no initialized linopy model. Run n.optimize.create_model() first.")
            
        m = network.model
        max_cost = optimal_cost * (1.0 + slack_pct)
        
        # Add a slack constraint bounding the original objective function
        m.add_constraints(m.objective.expression <= max_cost, name="mga_slack_cost_limit")
        print(f"Added MGA cost slack limit <= {max_cost}")

    def set_alternative_objective(
        self, 
        network: pypsa.Network, 
        target_component: str,
        target_carrier: str | None = None,
        target_action: str = 'minimize'
    ) -> None:
        """
        Dynamically constructs an MGA objective function based on component, carrier, and direction.
        Allows the user to minimize or maximize specific infrastructure builds (e.g., maximize H2 links, minimize AC lines).
        """
        if not hasattr(network, "model"):
            raise ValueError("Network has no initialized linopy model.")
            
        m = network.model
        
        # Map PyPSA component struct to nominal capacity variable strings in Linopy
        cap_var_map = {
            "Generator": "Generator-p_nom",
            "Store": "Store-e_nom",
            "StorageUnit": "StorageUnit-p_nom",
            "Link": "Link-p_nom",
            "Line": "Line-s_nom"
        }
        
        if target_component not in cap_var_map:
            raise ValueError(f"Target component '{target_component}' is not supported for dynamic capacity MGA sweeps.")
            
        var_name = cap_var_map[target_component]
        print(f"  [MGA Engine] Constructing alternative objective over linopy variable: {var_name}")
        
        try:
            target_vars = m.variables[var_name]
        except KeyError:
            print(f"  [WARNING] Variable {var_name} not found in optimization model. MGA sweep aborted.")
            return

        # Dynamically subset the network dataframe by carrier if specified
        df = network.df(target_component)
        
        if target_carrier:
            target_series = df[df.carrier == target_carrier].index
            if target_series.empty:
                print(f"  [WARNING] No {target_component} found matching carrier '{target_carrier}'. MGA sweep aborted.")
                return
            filtered_vars = target_vars.loc[target_series]
            print(f"  [MGA Engine] Filtering target to {len(target_series)} {target_component}(s) matching carrier={target_carrier}.")
        else:
            filtered_vars = target_vars
            print(f"  [MGA Engine] Targeting all {len(df)} {target_component}(s).")

        # Construct the summation expression
        new_obj = filtered_vars.sum()
        
        # Apple optimization direction
        if target_action.lower() == "maximize":
            m.objective = -new_obj
        else:
            m.objective = new_obj
            
        print(f"  [MGA SUCCESS] Set absolute {target_action.upper()} objective for {target_component} {f'(carrier: {target_carrier})' if target_carrier else ''}.")
