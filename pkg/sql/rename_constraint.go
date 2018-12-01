// Copyright 2017 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package sql

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
)

var errEmptyConstraintName = pgerror.NewError(pgerror.CodeSyntaxError, "empty constraint name")

type renameConstraintNode struct {
	n         *tree.RenameConstraint
	tableDesc *sqlbase.MutableTableDescriptor
}

// RenameConstraint renames the constraint.
// Privileges: CREATE on table.
//	 notes: postgres requires CREATE on the table.
//					mysql requires ALTER, CREATE, INSERT on the table.
func (p *planner) RenameConstraint(ctx context.Context, n *tree.RenameConstraint) (planNode, error) {
	// Check if table exists.
	tableDesc, err := p.ResolveMutableTableDescriptor(ctx, &n.Table, !n.IfExists, requireTableDesc)
	if err != nil {
		return nil, err
	}
	if tableDesc == nil {
		return newZeroNode(nil /* columns */), nil
	}

	if err := p.CheckPrivilege(ctx, tableDesc, privilege.CREATE); err != nil {
		return nil, err
	}

	return &renameConstraintNode{n: n, tableDesc: tableDesc}, nil
}

func (n *renameConstraintNode) startExec(params runParams) error {
	p := params.p
	ctx := params.ctx
	tableDesc := n.tableDesc

	// If no new name is given, return an error.
	if n.n.NewName == "" {
		return errEmptyConstraintName
	}

	// Fetch a list of all constraints for this table.
	constraintsList, err := tableDesc.GetConstraintInfo(ctx, nil)

	// If the constraint whose name is to be changed doesn't exist yet, return an error.
	if _, ok := constraintsList[string(n.n.Name)]; !ok {
		return fmt.Errorf("constraint %s does not exist", string(n.n.Name))
	}

	// If the new name for the constraint is identical to an existing constraint, return an error.
	if _, ok := constraintsList[string(n.n.NewName)]; !ok {
		return fmt.Errorf("constraint name %s already exists", string(n.n.NewName))
	}

	if err := tableDesc.Validate(ctx, p.txn, p.EvalContext().Settings); err != nil {
		return err
	}

	return
}
